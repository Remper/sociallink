import tensorflow as tf

from tensorflow.contrib import slim

from pairwise_models.simple import SimpleModel
from pairwise_models.model import Model

DEFAULT_LAYERS = 5
DEFAULT_UNITS = 256
DEFAULT_BATCH_SIZE = 256
DEFAULT_MAX_EPOCHS = 100

DEFAULT_LEARNING_RATE = 1e-4
DEFAULT_DROPOUT_RATE = 0.8


class EmbExtraLayer(SimpleModel):
    def __init__(self, name, inputs, classes):
        SimpleModel.__init__(self, name, inputs, classes)

    def _definition(self):
        graph = tf.Graph()
        with graph.as_default():
            # Graph begins with input. tf.placeholder tells TF that we will input those variables at each iteration
            self._train_features = dict()
            feature_list = []
            input_size = 0

            # Getting all appropriate subspaces for this model
            kb_emb = None
            sg_emb = None
            emb_size = None
            self._train_labels = tf.placeholder(tf.float32, shape=[None, self._classes], name="Y")
            for id, length in self._inputs.items():
                self._train_features[id] = tf.placeholder(tf.float32, shape=[None, length], name="X-"+id)
                if id.startswith("emb_kb"):
                    if kb_emb is not None:
                        raise Exception("Two embeddings for knowledge base detected")
                    kb_emb = self._train_features[id]
                    emb_size = length
                    continue
                if id.startswith("emb_sg"):
                    if sg_emb is not None:
                        raise Exception("Two embeddings for social graph detected")
                    sg_emb = self._train_features[id]
                    emb_size = length
                    continue

                feature_list.append(self._train_features[id])
                input_size += length

            if sg_emb is None or kb_emb is None:
                raise Exception("Both KB and SG embeddings required for this model")

            # Dropout rate
            self._dropout_rate = tf.placeholder(tf.float32, name="dropout_rate")

            # Embeddings multiplication
            with tf.name_scope("emb_dense_transform"):
                kb_emb = self.dense(kb_emb, emb_size, emb_size, self._dropout_rate)
            with tf.name_scope("emb_dense_transform"):
                sg_emb = self.dense(sg_emb, emb_size, emb_size, self._dropout_rate)
            with tf.name_scope("emb_cosine_similarity"):
                emb_feat = tf.reduce_sum(tf.multiply(
                    tf.nn.l2_normalize(kb_emb, axis=0),
                    tf.nn.l2_normalize(sg_emb, axis=0)
                ), axis=1, keepdims=True)
            feature_list.append(emb_feat)
            input_size += 1

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
        model = EmbExtraLayer(params["name"], params["inputs"], params["classes"])
        model.layers(params["layers"]).units(params["units"])
        return model
