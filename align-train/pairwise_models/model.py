import json

from os import path
from sklearn.utils import shuffle

import tensorflow as tf
import time
import numpy as np


class Model:
    def __init__(self, name):
        self._name = name
        self._graph = None
        self._session = None
        self._saver = None
        self._ready = False

    def _definition(self):
        raise NotImplementedError("Definition function must be implemented for the model")

    def _init(self):
        if self._session is not None:
            return

        self._graph = self._definition()
        if self._saver is None:
            raise NotImplementedError("Definition wasn't properly implemented: missing saver")

        gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.49)
        self._session = tf.Session(graph=self._graph, config=tf.ConfigProto(gpu_options=gpu_options))

    def train(self, train_prod, eval_prod=None):
        raise NotImplementedError("Training function has to be defined")

    def predict(self, features):
        raise NotImplementedError("Predict function has to be defined")

    def restore_from_file(self, filename):
        self._init()
        self._saver.restore(self._session, filename+'.cpkt')
        self._ready = True

    def _check_if_ready(self):
        if not self._ready:
            raise ValueError("Model isn't ready for prediction yet, train it or restore from file first")

    def save_to_file(self, filename):
        self._check_if_ready()
        print("Saving model")
        timestamp = time.time()
        self._saver.save(sess=self._session, save_path=path.join(filename,'model.cpkt'))
        print("Done in %.2fs" % (time.time() - timestamp))

    @staticmethod
    def weight_variable(shape):
        initial = tf.truncated_normal(shape, stddev=0.1)
        return tf.Variable(initial)

    @staticmethod
    def bias_variable(shape):
        initial = tf.constant(0.1, shape=shape)
        return tf.Variable(initial)


class BatchProducer:
    def __init__(self, filename):
        self.feature_space = 0
        self.labels = {}
        self.set_size = 0
        self.filename = filename
        self.randomisation = True

        print("Figuring out dataset metadata")
        timestamp = time.time()
        self.feature_space, self.labels, self.set_size = self._get_dataset_metadata()
        print("Done in %.2fs" % (time.time() - timestamp))
        self._print_stats()

    def random_off(self):
        print("Disabling randomisation for BatchProducer")
        self.randomisation = False

    def produce(self, batch_size: int) -> (np.ndarray, np.ndarray, int):
        """
            Produces full batch ready to be input in NN
        """
        pass

    def _print_stats(self):
        print("Feature space: %s. Classes: %d. Training set size: %d (%s)"
              % (", ".join(["%s(%d)" % (id, length) for id, length in self.feature_space.items()]),
                 len(self.labels), self.set_size,
                 ", ".join(["%d: %d" % (id, count) for id, count in self.labels.items()])))

    def _train_set_reader(self):
        raise NotImplementedError("You should implement a reader function for the batch producer")

    def _get_dataset_metadata(self):
        """
            Figures out amount of labels and features
        """
        feature_space = dict()
        labels = dict()
        set_size = 0
        for raw_label, features in self._train_set_reader():
            if raw_label not in labels:
                labels[raw_label] = 0
            labels[raw_label] += 1
            for subspace in features:
                if subspace not in feature_space:
                    feature_space[subspace] = len(features[subspace])
                    continue

                if feature_space[subspace] == len(features[subspace]):
                    continue

                raise RuntimeError("Inconsistent feature sets in the dataset (subspace: %s, recorded: %d, found: %d)"
                                   % (subspace, feature_space[subspace], features[subspace]))
            set_size += 1

        return feature_space, labels, set_size


class JSONBatchProducer(BatchProducer):
    def __init__(self, filename):
        super().__init__(filename)

    def produce(self, batch_size: int) -> (dict, np.ndarray, int):
        """
            Produces full batch ready to be input in NN
        """
        labels = list()
        batch = dict()

        reader = self._train_set_reader()
        cur_sample = 0
        while True:
            try:
                while len(labels) < batch_size:
                    raw_label, features = reader.__next__()
                    label = np.zeros(len(self.labels), dtype=np.float32)
                    label[raw_label] = 1.0

                    labels.append(label)
                    self._append(batch, features)
                yield self._stack_and_clean(batch), np.vstack(labels), cur_sample
                cur_sample += len(batch)
                labels = list()
            except:
                break
        return self._stack_and_clean(batch), np.vstack(labels), cur_sample

    def _stack_and_clean(self, bc: dict) -> dict:
        stacked = dict()
        for subspace in bc:
            stacked[subspace] = np.vstack(bc[subspace])
            bc[subspace] = []
        return stacked

    def _append(self, bc: dict, vector: dict) -> None:
        for subspace in vector:
            if subspace not in bc:
                bc[subspace] = []
            bc[subspace].append(vector[subspace])

    def _train_set_reader(self) -> (int, dict):
        """
            Reads training set one sample at the time
        """
        with open(self.filename, 'r') as reader:
            for line in reader:
                row = line.rstrip().split('\t')
                features = json.loads(row[1])
                yield int(row[0]), features


class PreloadedJSONBatchProducer(JSONBatchProducer):
    def __init__(self, filename):
        super().__init__(filename)

        print("Preloading dataset")
        timestamp = time.time()
        self.X, self.Y = self._load_dataset()
        print("Done in %.2fs" % (time.time() - timestamp))

    def index(self, batch, fr, to=None):
        result = dict()
        for subspace in batch:
            if to is not None:
                result[subspace] = batch[subspace][fr:to]
            else:
                result[subspace] = batch[subspace][fr:]
        return result

    def resample(self) -> (dict, np.ndarray):
        keys = self.X.keys()
        arrays = []
        for key in keys:
            arrays.append(self.X[key])
        arrays.append(self.Y)
        arrays = shuffle(*arrays)

        order = 0
        X = dict()
        for key in keys:
            X[key] = arrays[order]
            order += 1
        Y = arrays[order]
        return X, Y

    def produce(self, batch_size: int) -> (dict, np.ndarray, int):
        """
            Produces full batch ready to be input in NN
        """

        if self.randomisation:
            X, Y = self.resample()
        else:
            X = self.X
            Y = self.Y
        size = Y.shape[0]

        pointer = 0
        while pointer + batch_size < size:
            yield self.index(X, pointer, pointer + batch_size), Y[pointer:pointer + batch_size], pointer
            pointer += batch_size
        yield self.index(X, pointer), Y[pointer:], pointer

    def _load_dataset(self):
        """
            Reads training set one by one
        """
        X = dict()
        Y = []
        X_batch = dict()
        Y_batch = []
        cutoff = 100000
        for raw_label, features in self._train_set_reader():
            label = np.zeros(len(self.labels), dtype=np.float32)
            label[raw_label] = 1.0

            self._append(X_batch, features)
            Y_batch.append(label)

            if len(X_batch) >= cutoff:
                self._append(X, self._stack_and_clean(X_batch))
                Y.append(np.vstack(Y_batch))
                X_batch = []
                Y_batch = []
        if len(X_batch) > 0:
            self._append(X, self._stack_and_clean(X_batch))
            Y.append(np.vstack(Y_batch))
        return self._stack_and_clean(X), np.vstack(Y)
