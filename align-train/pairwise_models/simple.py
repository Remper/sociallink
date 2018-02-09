import time
import tensorflow as tf
import numpy as np
from flask import json
from os import path
from sklearn.metrics import confusion_matrix

from pairwise_models.model import Model, BatchProducer
from tensorflow.contrib import slim

DEFAULT_LAYERS = 5
DEFAULT_UNITS = 256
DEFAULT_BATCH_SIZE = 256
DEFAULT_MAX_EPOCHS = 100

DEFAULT_LEARNING_RATE = 1e-4
DEFAULT_DROPOUT_RATE = 0.8


def precision(tp: int, fp: int) -> float:
    return tp / (tp + fp)


def recall(tp: int, fn: int) -> float:
    return tp / (tp + fn)


def f1(tp: int, fp: int, fn: int) -> float:
    prec = precision(tp, fp)
    rec = recall(tp, fn)
    return 2 * prec * rec / (prec + rec)


class SimpleModel(Model):
    def __init__(self, name, inputs, classes, use_features=None):
        Model.__init__(self, name)
        if use_features is not None:
            features = dict()
            for subspace in use_features:
                features[subspace] = inputs[subspace]
            self._inputs = features
        else:
            self._inputs = inputs
        self._classes = classes
        self.batch_size(DEFAULT_BATCH_SIZE).units(DEFAULT_UNITS).layers(DEFAULT_LAYERS)\
            .max_epochs(DEFAULT_MAX_EPOCHS).learning_rate(DEFAULT_LEARNING_RATE)\
            .l1(False).tolerance(False)

    def batch_size(self, batch_size):
        self._batch_size = batch_size
        return self

    def units(self, units):
        self._units = units
        return self

    def layers(self, layers):
        self._layers = layers
        return self

    def max_epochs(self, max_epochs):
        self._max_epochs = max_epochs
        return self

    def l1(self, l1: bool):
        self._l1 = l1
        return self

    def tolerance(self, tolerance: bool):
        self._tolerance = tolerance
        return self

    def learning_rate(self, learning_rate):
        self._learning_rate = learning_rate
        return self

    def dense(self, input, input_size, out_size, dropout):
        with tf.name_scope("dense_layer"):
            weights = self.weight_variable([input_size, out_size])
            biases = self.bias_variable([out_size])
            hidden = tf.nn.relu(tf.matmul(input, weights) + biases)
            out = tf.nn.dropout(hidden, dropout)
        return out

    def _definition(self):
        graph = tf.Graph()
        with graph.as_default():
            # Graph begins with input. tf.placeholder tells TF that we will input those variables at each iteration
            self._train_features = dict()
            feature_list = []
            input_size = 0
            for id, length in self._inputs.items():
                self._train_features[id] = tf.placeholder(tf.float32, shape=[None, length], name="X-"+id)
                feature_list.append(self._train_features[id])
                input_size += length
            self._train_labels = tf.placeholder(tf.float32, shape=[None, self._classes], name="Y")

            # Dropout rate
            self._dropout_rate = tf.placeholder(tf.float32)

            # Multiple dense layers
            hidden_units = self._units
            layer = tf.concat(feature_list, 1, name="subspace-stitching")
            for idx in range(self._layers):
                layer = self.dense(layer, input_size, hidden_units, self._dropout_rate)
                input_size = hidden_units

            # Linear layer before softmax
            weights = self.weight_variable([input_size, self._classes])
            biases = self.bias_variable([self._classes])
            layer = tf.matmul(layer, weights) + biases


            # Softmax and cross entropy in the end
            losses = tf.nn.softmax_cross_entropy_with_logits_v2(labels=self._train_labels, logits=layer)
            self._loss = tf.reduce_mean(losses)

            # L1 regularization
            if self._l1:
                l1_regularizer = tf.contrib.layers.l1_regularizer(
                    scale=0.0005 / (self._units * self._layers), scope=None
                )
                weights = tf.trainable_variables()
                l1_reg = tf.contrib.layers.apply_regularization(l1_regularizer, weights)
                self._loss += l1_reg

            self._prediction = tf.nn.softmax(layer)
            tf.summary.scalar("loss", self._loss)
            self._global_step = tf.train.get_or_create_global_step()
            self._optimizer = slim.optimize_loss(loss=self._loss, global_step=self._global_step, learning_rate=None,
                                                 optimizer=tf.train.AdamOptimizer(learning_rate=self._learning_rate),
                                                 clip_gradients=5.0)
            self._saver = tf.train.Saver()

            # Evaluation
            self._results = tf.argmax(layer, axis=1)
        return graph

    def train(self, train_prod: BatchProducer, eval_prod: BatchProducer = None):
        self._init()

        with self._session.as_default():
            # Main execution
            check_interval = 500
            # Initializing everything
            writer = tf.summary.FileWriter(logdir="logs", graph=self._graph)
            print("Initializing variables")
            timestamp = time.time()
            with self._graph.as_default():
                g_summary = tf.summary.merge_all()
                tf.global_variables_initializer().run()
            print("Done in %.5fs" % (time.time() - timestamp))
            print("Starting training")

            # Main execution loop
            average_loss = 0
            timestamp = time.time()
            tolerance_margin = 5120 // self._batch_size
            if tolerance_margin < 20:
                tolerance_margin = 20
            tolerance = tolerance_margin + 1
            min_loss = -1
            for cur_epoch in range(self._max_epochs):
                for features, labels, pointer in train_prod.produce(self._batch_size):
                    if self._tolerance and tolerance == 0:
                        break

                    run_metadata = tf.RunMetadata()

                    feed = dict()
                    for id, subspace in self._train_features.items():
                        feed[subspace] = features[id]
                    feed.update({
                        self._train_labels:   labels,
                        self._dropout_rate:   DEFAULT_DROPOUT_RATE
                    })
                    _, loss_value, global_step, summary_v = self._session.run(
                        [self._optimizer, self._loss, self._global_step, g_summary],
                        feed_dict=feed,
                        run_metadata=run_metadata)

                    # Writes loss_summary to log. Each call represents a single point on the plot
                    writer.add_run_metadata(run_metadata, 'step%d' % global_step)
                    writer.add_summary(summary=summary_v, global_step=global_step)

                    # Output average loss periodically
                    average_loss += loss_value
                    if global_step % check_interval == 0 and global_step > 0:
                        average_loss /= check_interval
                        if min_loss < average_loss:
                            tolerance -= 1
                        else:
                            if tolerance < tolerance_margin:
                                tolerance += 1
                        if min_loss > average_loss or min_loss == -1:
                            min_loss = average_loss

                        appendix = ""
                        if eval_prod is not None and len(eval_prod.labels) == 2 and global_step % (check_interval*3) == 0:
                            tp = 0
                            fp = 0
                            fn = 0
                            for test_X, true_Y, _ in eval_prod.produce(self._batch_size):
                                feed = dict()
                                for id, subspace in self._train_features.items():
                                    feed[subspace] = test_X[id]
                                feed.update({
                                    self._dropout_rate: 1.0
                                })
                                pred_Y = self._results.eval(
                                    session=self._session,
                                    feed_dict=feed)
                                try:
                                    _, cur_fp, cur_fn, cur_tp = confusion_matrix(np.argmax(true_Y, axis=1), pred_Y).ravel()
                                    tp += cur_tp
                                    fp += cur_fp
                                    fn += cur_fn
                                except ValueError as e:
                                    pass
                                except Exception as e:
                                    print(e)
                                    print(confusion_matrix(np.argmax(true_Y, axis=1), pred_Y).ravel())
                                    print(confusion_matrix(np.argmax(true_Y, axis=1), pred_Y))
                            appendix += ", P: %.2f%%, R: %.2f%%, F1: %.2f%%" % (
                                precision(tp, fp) * 100, recall(tp, fn) * 100, f1(tp, fp, fn) * 100
                            )

                        print("[+] step: %4.1fk, %6.2f steps/s, tol: %2d, epoch: %5.2f, avg.loss: %.5f, min.loss: %.5f%s"
                              % (float(global_step) / 1000, float(check_interval) / (time.time() - timestamp),
                                 tolerance, cur_epoch+(pointer/train_prod.set_size), average_loss, min_loss, appendix))
                        timestamp = time.time()
                        average_loss = 0
        if self._tolerance and tolerance <= 0:
            print("Tolerance margin reached")
        else:
            print("Amount of epochs reached")
        self._ready = True

    def predict(self, features):
        self._init()
        self._check_if_ready()

        feed = dict()
        for id, subspace in self._train_features.items():
            feed[subspace] = features[id].reshape(-1, self._inputs[id])
        feed.update({
            self._dropout_rate: 1.0
        })
        return self._prediction.eval(
            session=self._session,
            feed_dict=feed)

    @staticmethod
    def restore_definition(params: dict) -> Model:
        model = SimpleModel(params["name"], params["inputs"], params["classes"])
        return model

    def save_to_file(self, filename):
        Model.save_to_file(self, filename)
        json.dump({
            'name': self._name,
            'inputs': self._inputs,
            'classes': self._classes
        }, open(path.join(filename,'model.json'), 'w'))
