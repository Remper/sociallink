from os import path
from random import shuffle

import json
import os
import numpy as np


def present(workdir, file):
    if path.exists(path.join(workdir, file)):
        print(" ", "[+] %s exists" % file)
    else:
        raise Exception("[-] %s does not exist" % file)


def write_sample(fp, sample, sample_dict):
    fp.write('\n')
    fp.write(sample)
    fp.write(',')
    fp.write(','.join(sample_dict[sample]))


def features_to_dict(features: dict) -> dict:
    result = dict()
    for feature in features:
        result[feature] = features[feature].tolist()
    return result


def features_from_dict(input: dict) -> dict:
    result = dict()
    for feature in input:
        result[feature] = np.array(input[feature])
    return result


class Scaler:
    def __init__(self):
        self.mean = {}
        self.stddev = {}
        self.count = 0

    def fit(self, features: dict) -> dict:
        result = {}
        for feature_id in features:
            result[feature_id] = self.fit_subspace(features[feature_id], feature_id)
        return result

    def fit_subspace(self, features, subspace):
        return (np.array(features) - self.mean[subspace]) / self.stddev[subspace]

    def to_dict(self):
        result = dict()
        result["mean"] = features_to_dict(self.mean)
        result["stddev"] = features_to_dict(self.stddev)
        result["count"] = self.count
        return result

    @staticmethod
    def from_dict(input: dict):
        result = Scaler()
        result.count = input["count"]
        result.mean = features_from_dict(input["mean"])
        result.stddev = features_from_dict(input["stddev"])
        return result


class ScalerBuilder:
    def __init__(self):
        self.features = {}

    def add_features(self, features: dict) -> None:
        for feature_id in features:
            sample = np.array(features[feature_id])
            if feature_id not in self.features:
                self.features[feature_id] = []
            self.features[feature_id].append(sample)

    def bake(self) -> Scaler:
        scaler = Scaler()
        for feature_id in self.features:
            samples = self.features[feature_id]
            means = np.zeros(len(samples[0]))
            stddevs = np.zeros(len(samples[0]))
            scaler.count = len(samples)

            for sample in samples:
                means += sample
            means /= scaler.count
            scaler.mean[feature_id] = means

            for sample in samples:
                stddevs += (sample - means)**2
            stddevs /= scaler.count
            stddevs = np.sqrt(stddevs)

            stddevs[stddevs == 0.0] = 1.0
            scaler.stddev[feature_id] = stddevs

        return scaler


def write_feature_sample(fp, sample, not_first: set, scaler: Scaler, balance=False):
    negatives = 0
    for i, features in enumerate(sample["features"]):
        correct = sample["candidates"][i]["profile"]["screenName"].casefold() == sample["entry"]["twitterId"].casefold()
        if not correct:
            negatives += 1
            if negatives > 5: # and balance:
                continue
        if fp in not_first:
            fp.write('\n')
        else:
            not_first.add(fp)

        if correct:
            fp.write("1")
        else:
            fp.write("0")

        fp.write('\t')
        fp.write(json.dumps(features_to_dict(scaler.fit(features))))


def load_dataset_and_produce_folds(workdir: str, num_folds: int):
    samples = []
    sample_dict = {}
    header = None
    with open(path.join(workdir, "gold.csv"), 'r') as reader:
        for line in reader:
            line = line.rstrip()
            if header is None:
                header = line
                continue
            line = line.split(",")
            entity = line[0]
            samples.append(entity)
            sample_dict[entity] = line[1:]
    print("Loaded %d samples" % len(samples))
    shuffle(samples)

    folds = [set() for _ in range(num_folds)]
    for i, sample in enumerate(samples):
        fold = i % num_folds
        folds[fold].add(sample)
    print("Generated folds with amounts: [%s]" % ", ".join([str(len(fold)) for fold in folds]))

    splits_dir = path.join(workdir, "splits")
    os.mkdir(splits_dir)

    test_writers = []
    train_writers = []
    for i, fold in enumerate(folds):
        split_dir = path.join(splits_dir, str(i))
        os.mkdir(split_dir)
        writer = open(path.join(split_dir, "test.csv"), 'w')
        writer.write(header)
        test_writers.append(writer)
        writer = open(path.join(split_dir, "train.csv"), 'w')
        writer.write(header)
        train_writers.append(writer)
    for i, fold in enumerate(folds):
        for sample in fold:
            for j in range(len(folds)):
                if i == j:
                    write_sample(test_writers[j], sample, sample_dict)
                else:
                    write_sample(train_writers[j], sample, sample_dict)
    [writer.close() for writer in test_writers+train_writers]

    print("Making the first pass on a dataset to collect scaler statistics")
    scaler_builders = []
    for i in range(len(folds)):
        scaler_builders.append(ScalerBuilder())
    counter = 0
    with open(path.join(workdir, "dataset.json"), 'r') as reader:
        for line in reader:
            sample = json.loads(line)
            for i in range(len(folds)):
                if not sample["entry"]["resourceId"] in folds[i]:
                    for features in sample["features"]:
                        scaler_builders[i].add_features(features)
            counter += 1
            if counter % 5000 == 0:
                print("  Done %d samples" % counter)

    print("Generating scalers")
    scalers = []
    for i in range(len(folds)):
        scaler = scaler_builders[i].bake()
        split_dir = path.join(splits_dir, str(i))
        json.dump(scaler.to_dict(), open(path.join(split_dir, "scaler.json"), 'w'))
        scalers.append(scaler)
        print("  Done scaler %d" % i)
    del scaler_builders

    print("Dumping train and test sets")
    test_writers = []
    train_writers = []
    for i, fold in enumerate(folds):
        split_dir = path.join(splits_dir, str(i))
        test_writers.append(open(path.join(split_dir, "test.json"), 'w'))
        train_writers.append(open(path.join(split_dir, "train.json"), 'w'))
    not_first = set()
    counter = 0
    with open(path.join(workdir, "dataset.json"), 'r') as reader:
        for line in reader:
            sample = json.loads(line)
            for i in range(len(folds)):
                if sample["entry"]["resourceId"] in folds[i]:
                    write_feature_sample(test_writers[i], sample, not_first, scalers[i])
                else:
                    write_feature_sample(train_writers[i], sample, not_first, scalers[i], True)
            counter += 1
            if counter % 5000 == 0:
                print("  Done %d samples" % counter)
    [writer.close() for writer in test_writers+train_writers]
