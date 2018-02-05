from __future__ import absolute_import
from __future__ import print_function

import argparse

from models.simple import SimpleModel
from models.model import JSONBatchProducer, PreloadedJSONBatchProducer

"""
    Basic deep neural network that works with SVM input
"""

parser = argparse.ArgumentParser(description='Basic deep neural network that works with SVM input')
parser.add_argument('--train', default='', required=True, help='Location of the training set', metavar='#')
parser.add_argument('--eval', default=None, help='Location of the evaluation set', metavar='#')
parser.add_argument('--output', default='', required=True, help='Save model to', metavar='#')
parser.add_argument('--max_epochs', default=100, help='Maximum amount of epochs', metavar='#')
parser.add_argument('--layers', default=5, help='Amount of hidden layers', metavar='#')
parser.add_argument('--units', default=256, help='Amount of hidden units per layer', metavar='#')
parser.add_argument('--batch_size', default=64, help='Amount of samples in a batch', metavar='#')
parser.add_argument('--preload', default=False, action='store_true', help='Preload datasets into memory')
parser.add_argument('--tolerance', default=False, action='store_true', help='Use tolerance margin to determine the end of training')
parser.add_argument('--l1', default=False, action='store_true', help='Use l1 regularization')
parser.add_argument('--main_feature', default=None, help='Train models for main feature set, main+each and all', metavar='#')
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
        print("  ", subspace, batch[subspace][idx])
    print("  Label: ", labels[idx])
    print("")

model = SimpleModel("Model", train_prod.feature_space, len(train_prod.labels))
model.units(args.units).layers(args.layers).batch_size(args.batch_size)\
    .max_epochs(args.max_epochs).tolerance(args.tolerance).l1(args.l1)
model.train(train_prod=train_prod, eval_prod=eval_prod)
model.save_to_file(args.output)

print("Done")
