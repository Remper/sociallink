import argparse
import numpy as np

from flask import Flask, request, json

from models import restore_definition

app = Flask(__name__)
model = None
args = None


@app.route("/predict")
def predict():
    try:
        features = request.args['features']
        if features is None:
            raise ValueError('provide a list of features')
        features = json.loads(features)
        for subspace in features:
            features[subspace] = np.array(features[subspace])
        result = model.predict(features=features)
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
    args = parser.parse_args()

    print("Initialized with settings:")
    print(vars(args))

    print("Loading model")

    model = restore_definition(args.input)
    model.restore_from_file(args.input)

    print("Starting webserver")
    app.run(port=int(args.port))
