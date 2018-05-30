import argparse
import numpy as np

from flask import Flask, request, json

from pairwise_models import restore_definition

app = Flask(__name__)
model = None
scaler = None
args = None


def _get_features(request, rescale=False):
    if rescale and model is None:
        raise ValueError('scaler is needed for this method to work')
    features = request.args['features']
    if features is None:
        raise ValueError('provide a list of features')
    features = json.loads(features)
    for subspace in features:
        subspace_values = np.array(features[subspace])
        if rescale:
            stddevs = scaler[subspace]["stddev"]
            means = scaler[subspace]["mean"]
            subspace_values -= means
            subspace_values /= stddevs
        features[subspace] = subspace_values

    return features


@app.route("/predict")
def predict():
    try:
        result = model.predict(features=_get_features(request))
        return json.jsonify(result.reshape(-1).tolist())
    except Exception as e:
        print(e)
        return json.jsonify({
            'result': 'error',
            'message': e.message
        })


@app.route("/predict/scaled")
def predict_scaled():
    try:
        result = model.predict(features=_get_features(request, True))
        return json.jsonify(result.reshape(-1).tolist())
    except Exception as e:
        print(e)
        return json.jsonify({
            'result': 'error',
            'message': e.message
        })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple API that returns predictions from a model')
    parser.add_argument('--input', default='model', help='Input model', metavar='#')
    parser.add_argument('--port', default='5000', help='Port to listen', metavar='#')
    parser.add_argument('--scaler', help='Location of the scaler file for this model', metavar='#')
    args = parser.parse_args()

    print("Initialized with settings:")
    print(vars(args))

    print("Loading model")

    model = restore_definition(args.input)
    model.restore_from_file(args.input)

    if args.scaler is not None:
        print("Loading the scaler")
        with open(args.scaler, 'r') as reader:
            scaler = json.load(reader)
            print("Scaler subspaces:", ", ".join(scaler.keys()))
            for subspace in scaler:
                stddevs = np.array(scaler[subspace]["stddev"])
                # Replace zeroes with ones to avoid dividing by zero in cases where there is zero variance
                stddevs[stddevs == 0] = 1
                scaler[subspace]["stddev"] = stddevs
                scaler[subspace]["mean"] = np.array(scaler[subspace]["mean"])


    print("Starting webserver")
    app.run(port=int(args.port))
