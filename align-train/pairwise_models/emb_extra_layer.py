import tensorflow as tf

from pairwise_models.model import Model
from tensorflow.contrib import slim

from pairwise_models.simple import SimpleModel


class EmbExtraLayer(SimpleModel):
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
            kb_emb = None
            sg_emb = None
            kb_emb_size = None
            sg_emb_size = None
            self._train_labels = tf.placeholder(tf.float32, shape=[None, self._classes], name="Y")
            for id, length in self._inputs.items():
                self._train_features[id] = tf.placeholder(tf.float32, shape=[None, length], name="X-"+id)
                if id.startswith("emb_kb"):
                    if kb_emb is not None:
                        raise Exception("Two embeddings for knowledge base detected")
                    kb_emb = self._train_features[id]
                    kb_emb_size = length
                    continue
                elif id.startswith("emb_sg"):
                    if sg_emb is not None:
                        raise Exception("Two embeddings for social graph detected")
                    sg_emb = self._train_features[id]
                    sg_emb_size = length
                    continue

                feature_list.append(self._train_features[id])
                input_size += length

            if sg_emb is None and kb_emb is None:
                raise Exception("Either KB or SG embedding is required for this model")

            # Dropout rate
            self._dropout_rate = tf.placeholder(tf.float32, name="dropout_rate")

            # Embeddings multiplication
            final_emb_size = 50
            if kb_emb is not None:
                with tf.name_scope("kb_dense_transform"):
                    kb_emb = self.dense(kb_emb, kb_emb_size, final_emb_size, self._dropout_rate)
                    feature_list.append(kb_emb)
                    input_size += final_emb_size
            if sg_emb is not None:
                with tf.name_scope("sg_dense_transform"):
                    sg_emb = self.dense(sg_emb, sg_emb_size, final_emb_size, self._dropout_rate)
                    feature_list.append(sg_emb)
                    input_size += final_emb_size
            if sg_emb is not None and kb_emb is not None:
                emb_feat = tf.multiply(kb_emb, sg_emb, name="emb_combination")
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
        model = EmbExtraLayer(params["name"], params["inputs"], params["classes"])
        model.layers(params["layers"]).units(params["units"])
        return model
