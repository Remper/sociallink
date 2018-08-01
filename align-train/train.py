from __future__ import absolute_import
from __future__ import print_function

import time

import argparse
import json
from multiprocessing import Lock, Queue
from multiprocessing.pool import Pool

from os import path, mkdir
from utils.common import present, load_dataset_and_produce_folds

from pairwise_models import get_custom_models
from pairwise_models.model import JSONBatchProducer, PreloadedJSONBatchProducer

"""
    Training pipeline for the SocialLink
      expects the complete set of samples from the feature extraction pipeline,
      generates splits and does n-fold cross-validation
"""

parser = argparse.ArgumentParser(description='Basic deep neural network that works with SVM input')
parser.add_argument('--work_dir', default='', required=True, metavar='#',
                    help='A working directory after the SocialLink\'s Evaluate pipeline script')
parser.add_argument('--max_epochs', default=100, metavar='#',
                    help='Maximum amount of epochs')
parser.add_argument('--layers', default=5, metavar='#',
                    help='Amount of hidden layers')
parser.add_argument('--units', default=256, metavar='#',
                    help='Amount of hidden units per layer')
parser.add_argument('--batch_size', default=64, metavar='#',
                    help='Amount of samples in a batch')
parser.add_argument('--folds', default=5, metavar='#',
                    help='Amount of folds for the cross validation')
parser.add_argument('--num_gpus', default=2, metavar='#',
                    help='Amount of GPUs to use')
parser.add_argument('--preload', default=False, action='store_true',
                    help='Preload datasets into memory')
parser.add_argument('--tolerance', default=False, action='store_true',
                    help='Use tolerance margin to determine the end of training')
parser.add_argument('--l1', default=False, action='store_true',
                    help='Use l1 regularization')
parser.add_argument('--l2', default=False, action='store_true',
                    help='Use l2 regularization')
parser.add_argument('--no_randomisation', default=False, action='store_true',
                    help='Disable randomisation')
args = parser.parse_args()

args.batch_size = int(args.batch_size)
args.units = int(args.units)
args.layers = int(args.layers)
args.max_epochs = int(args.max_epochs)
args.folds = int(args.folds)
args.num_gpus = int(args.num_gpus)

print("Initialized with settings:")
print(vars(args))

print("")
print("Checking the existence of the required files: ")
[present(args.work_dir, file) for file in ["dataset.json", "manifest.json", "gold.csv"]]
print("")

if not path.exists(path.join(args.work_dir, "splits")):
    print("")
    print("============================")
    print("===== Generating folds =====")
    print("============================")
    print("")
    load_dataset_and_produce_folds(args.work_dir, args.folds)
else:
    print("Folds had already been generated. Skipping")


models = get_custom_models()
feature_manifest = json.load(open(path.join(args.work_dir, "manifest.json")))
eval_results = Queue()
available_gpus = Queue()
lock = Lock()
for i in range(args.num_gpus):
    for _ in range(2):
        available_gpus.put(i)


class Settings:
    def __init__(self, fold_id, device):
        self.fold_id = fold_id
        self.device = device


def train(settings):
    fold_path = path.join(args.work_dir, "splits", str(settings.fold_id))
    train_path = path.join(fold_path, "train.json")
    eval_path = path.join(fold_path, "test.json")
    output_path = path.join(fold_path, "models")
    if not path.exists(output_path):
        mkdir(output_path)
    timestamp = time.time()

    print("")
    print("============================")
    print("==== Training split %3d ====" % settings.fold_id)
    print("============================")
    print("")
    print("Initializing dataset readers")
    try:
        Producer = JSONBatchProducer
        if args.preload:
            Producer = PreloadedJSONBatchProducer
        train_prod = Producer(train_path)
        eval_prod = Producer(eval_path)
        eval_prod.random_off()
        if args.no_randomisation:
            train_prod.random_off()
    except Exception as e:
        print(e)
        print("Paths: ", train_path, eval_path, output_path)
        raise(e)

    print("Test batch:")
    batch, labels, _ = train_prod.produce(2).__next__()
    for idx, _ in enumerate(labels):
        print("  Features: ")
        for subspace in batch:
            print("  ", subspace, batch[subspace][idx][:5], "(total: %d)" % len(batch[subspace][idx]))
        print("  Label: ", labels[idx])
        print("")

    # Acquire the GPU
    lock.acquire()
    device_id = available_gpus.get()
    lock.release()
    print("Acquired GPU with id %d (%d GPUs left)" % (device_id, available_gpus.qsize()))

    available_features = train_prod.feature_space.keys()
    fold_results = {}
    for manifest_job in feature_manifest:
        aligned_features = []
        use_features = []
        feature_set = manifest_job["features"]
        model_name = manifest_job["model"]
        for feature in feature_set:
            for avail_feature in available_features:
                # if avail_feature.startswith(feature):
                if avail_feature == feature:
                    aligned_features.append("%s -> %s" % (feature, avail_feature))
                    use_features.append(avail_feature)
                    break

        full_model_name = "("+str(settings.fold_id)+")"+model_name+"@"+"+".join(use_features)
        if path.exists(path.join(output_path, model_name, "+".join(use_features))):
            print("The model %s has already been trained, skipping" % full_model_name)
            continue

        try:
            print("Starting training model \"%s\" with the following feature set: %s"
                    % (model_name, ", ".join(aligned_features)))
            model = models[model_name](full_model_name, train_prod.feature_space, len(train_prod.labels), use_features=use_features)
            model.device(str(device_id)).units(args.units).layers(args.layers).batch_size(args.batch_size)\
                .max_epochs(args.max_epochs).tolerance(args.tolerance).l1(args.l1).l2(args.l2)
            eval_result = model.train(train_prod=train_prod, eval_prod=eval_prod)
            fold_results[full_model_name] = eval_result
            model_dir = path.join(output_path, model_name)
            if not path.exists(model_dir):
                mkdir(model_dir)
            model.save_to_file(path.join(model_dir, "+".join(use_features)))
        except Exception as e:
            print("Skipping: %s" % str(e))
    eval_results.put(fold_results)

    # Release the GPU
    lock.acquire()
    available_gpus.put(device_id)
    lock.release()
    print("Done in %.2fs" % (time.time() - timestamp))


with Pool(1*args.num_gpus) as p:
    p.map(train, [Settings(i, 0) for i in range(args.folds)])

print("")
print("============================")
print("======= Eval results =======")
print("============================")
print("")
print("Num results: %d" % eval_results.qsize())
while not eval_results.empty():
    fold_result = eval_results.get()

    for model_name in fold_result:
        print("  %s -> %s" % (model_name, fold_result[model_name]))
