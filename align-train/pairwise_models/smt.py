import tensorflow as tf

from pairwise_models.model import Model
from tensorflow.contrib import slim

from pairwise_models.simple import SimpleModel


class SMTModel(SimpleModel):
    """
        Simple model that doesn't require fancy features from KB side, but explicitly merges textual features
        Designed to work with Social Media Toolkit
    """
    def __init__(self, name, inputs, classes, use_features=None):
        SimpleModel.__init__(self, name, inputs, classes, use_features=use_features)

    def _definition(self):
        graph = tf.Graph()
        with graph.as_default():
            # Graph begins with input. tf.placeholder tells TF that we will input those variables at each iteration
            self._train_features = dict()

            # Setting up label tensor
            self._train_labels = tf.placeholder(tf.float32, shape=[None, self._classes], name="Y")

            # Dropout rate
            self._dropout_rate = tf.placeholder(tf.float32, name="dropout_rate")

            feature_list = []
            input_size = 0

            # Getting all appropriate subspaces for this model
            text_pair = []
            sg_emb = None
            profile = None
            for id, length in self._inputs.items():
                self._train_features[id] = tf.placeholder(tf.float32, shape=[None, length], name="X-"+id)

                if id.startswith("text_"):
                    text_pair.append(id)
                    continue

                if id.startswith("emb_sg"):
                    sg_emb = id
                    continue

                if id.startswith("profile"):
                    profile = id
                    continue

                feature_list.append(self._train_features[id])
                input_size += length

            # Reduce dimensionality of aux user representation
            features = []
            if profile is not None:
                features.append(profile)
            if sg_emb is not None:
                features.append(sg_emb)
            if len(features) > 0:
                with tf.name_scope("user_transform"):
                    feature_tensors = [self._train_features[id] for id in features]
                    if len(features) > 1:
                        feature_tensor = tf.concat(feature_tensors, 1, name="user-stitching")
                    else:
                        feature_tensor = feature_tensors[0]
                    feature_tensor = self.dense(feature_tensor, sum([self._inputs[id] for id in features]), 50, self._dropout_rate)
                    feature_list.append(feature_tensor)
                    input_size += 50

            # Embeddings translation layers
            if len(text_pair) != 2:
                raise Exception("Both KB and social texts are required by this model")
            add_feats, add_input_size = self._add_translation_layer(text_pair[0], text_pair[1], 100)
            feature_list += add_feats
            input_size += add_input_size

            # Multiple dense layers
            hidden_units = self._units
            layer = tf.concat(feature_list, 1, name="subspace-stitching")
            for idx in range(self._layers):
                layer = self.dense(layer, input_size, hidden_units, self._dropout_rate)
                input_size = hidden_units

            # Linear layer before softmax
            with tf.name_scope("dense_output"):
                weights = self.weight_variable([input_size, self._classes])
                biases = self.bias_variable([self._classes])
                layer = tf.matmul(layer, weights) + biases

            # Softmax and cross entropy in the end
            losses = tf.nn.softmax_cross_entropy_with_logits_v2(labels=self._train_labels, logits=layer)
            self._loss = tf.reduce_mean(losses)

            # L1&L2 regularization
            self._add_regularization()

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

    @staticmethod
    def restore_definition(params: dict) -> Model:
        model = SMTModel(params["name"], params["inputs"], params["classes"])
        model.layers(params["layers"]).units(params["units"])
        return model
