from __future__ import absolute_import
from __future__ import print_function

import argparse
import traceback
from os import path, mkdir

from pairwise_models import get_custom_models
from pairwise_models.simple import SimpleModel
from pairwise_models.model import JSONBatchProducer, PreloadedJSONBatchProducer

"""
    Basic deep neural network that works with SVM input
"""

parser = argparse.ArgumentParser(description='Basic deep neural network that works with SVM input')
parser.add_argument('--train', default='', required=True, help='Location of the training set', metavar='#')
parser.add_argument('--eval', default=None, help='Location of the evaluation set', metavar='#')
parser.add_argument('--output_dir', default='', required=True, help='A directory to save the model to', metavar='#')
parser.add_argument('--max_epochs', default=100, help='Maximum amount of epochs', metavar='#')
parser.add_argument('--layers', default=5, help='Amount of hidden layers', metavar='#')
parser.add_argument('--units', default=256, help='Amount of hidden units per layer', metavar='#')
parser.add_argument('--batch_size', default=64, help='Amount of samples in a batch', metavar='#')
parser.add_argument('--preload', default=False, action='store_true', help='Preload datasets into memory')
parser.add_argument('--tolerance', default=False, action='store_true', help='Use tolerance margin to determine the end of training')
parser.add_argument('--l1', default=False, action='store_true', help='Use l1 regularization')
parser.add_argument('--l2', default=False, action='store_true', help='Use l2 regularization')
parser.add_argument('--no_randomisation', default=False, action='store_true', help='Disable randomisation')
args = parser.parse_args()

args.batch_size = int(args.batch_size)
args.units = int(args.units)
args.layers = int(args.layers)
args.max_epochs = int(args.max_epochs)

print("Initialized with settings:")
print(vars(args))

print("Initializing dataset readers")
Producer = JSONBatchProducer
if args.preload:
    Producer = PreloadedJSONBatchProducer
train_prod = Producer(args.train)
eval_prod = None
if args.eval:
    eval_prod = Producer(args.eval)
    eval_prod.random_off()
if args.no_randomisation:
    train_prod.random_off()

print("Test batch:")
batch, labels, _ = train_prod.produce(5).__next__()
for idx, _ in enumerate(labels):
    print("  Features: ")
    for subspace in batch:
        print("  ", subspace, batch[subspace][idx][:10], "(total: %d)" % len(batch[subspace][idx]))
    print("  Label: ", labels[idx])
    print("")

models = get_custom_models()
feature_manifest = [
    ['iswc17'],
    ['iswc17', 'emb_kb', 'emb_sg'],
    ['iswc17', 'emb_kb'],
    ['iswc17', 'emb_sg'],
    ['emb_kb', 'emb_sg']
]
available_features = train_prod.feature_space.keys()
eval_results = {}
for feature_set in feature_manifest:
    aligned_features = []
    use_features = []
    for feature in feature_set:
        for avail_feature in available_features:
            if avail_feature.startswith(feature):
                aligned_features.append("%s -> %s" % (feature, avail_feature))
                use_features.append(avail_feature)
                break

    for model_name in models:
        try:
            print("Starting training model \"%s\" with the following feature set: %s" % (model_name, ", ".join(aligned_features)))
            model = models[model_name](model_name, train_prod.feature_space, len(train_prod.labels), use_features=use_features)
            model.units(args.units).layers(args.layers).batch_size(args.batch_size)\
                .max_epochs(args.max_epochs).tolerance(args.tolerance).l1(args.l1).l2(args.l2)
            eval_result = model.train(train_prod=train_prod, eval_prod=eval_prod)
            eval_results[model_name+"@"+"+".join(use_features)] = eval_result
            model_dir = path.join(args.output_dir, model_name)
            if not path.exists(model_dir):
                mkdir(model_dir)
            model.save_to_file(path.join(model_dir, "+".join(use_features)))
        except Exception as e:
            print("Skipping: %s" % str(e))

print("Eval results:")
for model_name in eval_results:
    print("  %s -> %s" % (model_name, eval_results[model_name]))

