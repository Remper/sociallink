from __future__ import absolute_import
from __future__ import print_function

import argparse
from os import path

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
parser.add_argument('--main_feature', default=None, help='Train pairwise_models for main feature set, main+each and all', metavar='#')
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

print("Test batch:")
batch, labels, _ = train_prod.produce(2).__next__()
for idx, _ in enumerate(labels):
    print("  Features: ")
    for subspace in batch:
        print("  ", subspace, batch[subspace][idx][:25], "(total: %d)" % len(batch[subspace][idx]))
    print("  Label: ", labels[idx])
    print("")


feature_sets = [train_prod.feature_space.keys()]
if args.main_feature is not None:
    feature_sets.append([args.main_feature])
    for subspace in train_prod.feature_space:
        if subspace == args.main_feature:
            continue
        feature_sets.append([args.main_feature, subspace])

models = get_custom_models()
for model_name in models:
    print("Starting training special model: %s" % model_name)
    model = models[model_name](model_name, train_prod.feature_space, len(train_prod.labels))
    model.units(args.units).layers(args.layers).batch_size(args.batch_size)\
        .max_epochs(args.max_epochs).tolerance(args.tolerance).l1(args.l1)
    model.train(train_prod=train_prod, eval_prod=eval_prod)
    model.save_to_file(path.join(args.output_dir, model_name))

for feature_set in feature_sets:
    print("Starting training model with the following feature set:", ", ".join(feature_set))
    model = SimpleModel("SimpleModel", train_prod.feature_space, len(train_prod.labels), use_features=feature_set)
    model.units(args.units).layers(args.layers).batch_size(args.batch_size)\
        .max_epochs(args.max_epochs).tolerance(args.tolerance).l1(args.l1)
    model.train(train_prod=train_prod, eval_prod=eval_prod)
    model.save_to_file(path.join(args.output_dir, "_".join(feature_set)))

print("Done")
