from os import path

import os

import argparse
import numpy as np

from flask import Flask, request, json

from pairwise_models import restore_definition
from utils.common import Scaler

app = Flask(__name__)
models = []
scalers = []
args = None


def _get_features(request, rescale=False):
    if rescale and model is None:
        raise ValueError('scaler is needed for this method to work')
    features = request.args['features']
    if features is None:
        raise ValueError('provide a list of features')
    features = json.loads(features)
    for subspace in features:
        features[subspace] = scalers[0].fit_subspace(features[subspace], subspace)

    return features


@app.route("/predict")
def predict():
    try:
        scores = np.zeros([2])
        for model in models:
            scores += model.predict(features=_get_features(request)).reshape(-1)
        scores /= len(models)
        return json.jsonify(scores.tolist())
    except Exception as e:
        print(e)
        return json.jsonify({
            'result': 'error',
            'message': e.message
        })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple API that returns predictions from a model')
    parser.add_argument('--input', default='model', help='Directory with trained models', metavar='#')
    parser.add_argument('--port', default='5000', help='Port to listen', metavar='#')
    args = parser.parse_args()

    print("Initialized with settings:")
    print(vars(args))

    print("Loading splits")
    splits = []
    # Detecting splits
    for split in os.listdir(args.input):
        split_dir = path.join(args.input, split)
        if split.startswith(".") or not path.isdir(split_dir):
            continue

        if not path.exists(path.join(split_dir, "models")):
            print("Models are not trained for split %s. Halting" % split)
            exit(0)
        splits.append(split_dir)
    print("Detected %d splits" % len(splits))

    if len(splits) == 0:
        print("No splits detected, should be at least one")
        exit(0)

    models_dir = path.join(splits[0], "models")
    print("\nDetected model:")
    model_def = None
    for model in os.listdir(models_dir):
        model_dir = path.join(models_dir, model)
        if model.startswith(".") or not path.isdir(model_dir):
            continue

        for feature_set in os.listdir(model_dir):
            if feature_set.startswith("."):
                continue
            model_def = (model, feature_set)
            break

    if model_def is None:
        print(" ", "No models detected. Halting")
        exit(0)

    model_name = "%s@%s" % (model_def[0], model_def[1])
    print(" ", model_name)

    print("\nDeserialising scalers")
    scalers = []
    for split_id, split in enumerate(splits):
        with open(path.join(split, "scaler.json"), 'r') as scaler_reader:
            scalers.append(Scaler.from_dict(json.load(scaler_reader)))

    models = []
    print("\nDeserialising models")
    for split_id, split in enumerate(splits):
        model_location = path.join(split, "models",  model_def[0], model_def[1], "model")
        model = restore_definition(model_location)
        model.restore_from_file(model_location)
        models.append(model)

    print("Starting webserver")
    app.run(port=int(args.port))
