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
            feature_list = []
            input_size = 0

            # Getting all appropriate subspaces for this model
            kb_text = None
            user_text = None
            kb_text_size = None
            user_text_size = None
            self._train_labels = tf.placeholder(tf.float32, shape=[None, self._classes], name="Y")

            # Going through feature dimensions
            for id, length in self._inputs.items():
                self._train_features[id] = tf.placeholder(tf.float32, shape=[None, length], name="X-"+id)
                if id == "text_lsa_dbpedia":
                    kb_text = self._train_features[id]
                    kb_text_size = length
                    continue

                if id == "text_lsa_tweets":
                    user_text = self._train_features[id]
                    user_text_size = length
                    continue

                feature_list.append(self._train_features[id])
                input_size += length

            if kb_text is None or user_text_size is None:
                raise Exception("Both textual embeddings required for this model")

            # Dropout rate
            self._dropout_rate = tf.placeholder(tf.float32, name="dropout_rate")

            # Embeddings multiplication
            final_emb_size = 50
            with tf.name_scope("kb_text_transform"):
                kb_text = self.dense(kb_text, kb_text_size, final_emb_size, self._dropout_rate)
                feature_list.append(kb_text)
                input_size += final_emb_size
            with tf.name_scope("user_text_transform"):
                user_text = self.dense(user_text, user_text_size, final_emb_size, self._dropout_rate)
                feature_list.append(user_text)
                input_size += final_emb_size
            emb_feat = tf.multiply(kb_text, user_text, name="emb_combination")
            feature_list.append(emb_feat)
            input_size += final_emb_size

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
