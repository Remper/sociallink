import argparse, os, time, json
import numpy as np

from os import path

from pairwise_models import restore_definition
from utils.common import Scaler


def f1(prec: float, rec: float) -> float:
    return 2 * prec * rec / (prec + rec)


def present(workdir: str, file: str) -> None:
    if path.exists(path.join(workdir, file)):
        print(" ", "%s exists" % file)
    else:
        raise Exception("%s does not exist" % file)


def test_set(workdir: str):
    with open(path.join(workdir, "dataset.json"), 'r') as reader:
        for line in reader:
            sample = json.loads(line)
            del sample["resource"]
            yield sample


def main(workdir):
    print("Performing checks")
    [present(workdir, file) for file in ["dataset.json", "splits", "manifest.json"]]

    # Preparing the evaluation directory
    evaluation_dir = path.join(workdir, "inference")
    if not path.exists(evaluation_dir):
        os.mkdir(evaluation_dir)

    splits = []
    # Detecting splits
    splits_dir = path.join(args.workdir, "splits")
    for split in os.listdir(splits_dir):
        split_dir = path.join(splits_dir, split)
        if split.startswith(".") or not path.isdir(split_dir):
            continue

        if not path.exists(path.join(split_dir, "models")):
            print("Models are not trained for split %s. Halting" % split)
            return
        splits.append(split_dir)
    print("Detected %d splits" % len(splits))

    if len(splits) == 0:
        print("Train the models first. Halting")
        return

    models_dir = path.join(splits[0], "models")
    print("\nDetected models:")
    models = dict()
    for model in os.listdir(models_dir):
        model_dir = path.join(models_dir, model)
        if model.startswith(".") or not path.isdir(model_dir):
            continue

        for feature_set in os.listdir(model_dir):
            if feature_set.startswith("."):
                continue
            model_name = "%s@%s" % (model, feature_set)
            models[model_name] = (model, feature_set)
            print(" ", model_name)

    if len(models) == 0:
        print(" ", "No models detected. Halting")
        return

    print("\nDeserialising scalers")
    test_scalers = []
    for split_id, split in enumerate(splits):
        with open(path.join(split, "scaler.json"), 'r') as scaler_reader:
            test_scalers.append(Scaler.from_dict(json.load(scaler_reader)))

    print("\nInference:")
    for model_name in sorted(models.keys()):
        print("Inferring using model %s" % model_name)

        # Invoking a trained model from disk for each split
        model_instances = []
        model_type, feature_set = models[model_name]
        try:
            for split_id, split in enumerate(splits):
                model_location = path.join(split, "models", model_type, feature_set, "model")
                model = restore_definition(model_location)
                model.restore_from_file(model_location)
                model_instances.append(model)
        except Exception as e:
            print("Error happened while restoring model:", e)
            continue

        debug_writer = open(path.join(evaluation_dir, model_name + ".results"), 'w')

        counter = 0
        check_interval = 1000
        timestamp = time.time()
        predicted = 0
        for sample in test_set(workdir):
            counter += 1
            candidate_id = -1
            second_best = -1.0
            sample_features = None
            for features in sample["features"]:
                candidate_id += 1
                if sample_features is None:
                    sample_features = dict()
                    for subspace in features:
                        sample_features[subspace] = []
                for subspace in features:
                    cur_vector = test_scalers[0].fit_subspace(features[subspace], subspace)
                    sample_features[subspace].append(cur_vector)

            if len(sample["features"]) > 0:
                prediction = {
                    "entry": sample["entry"]["resourceId"],
                    "candidates": [],
                    "prediction": None
                }
                for subspace in sample_features:
                    sample_features[subspace] = np.vstack(sample_features[subspace])
                sample_scores = np.zeros([len(sample["features"])])
                for i in range(len(splits)):
                    sample_scores += model_instances[i].predict(features=sample_features)[::, 1]
                sample_scores /= len(splits)

                # Rescaling scores using Francesco's approach
                scores_sum = sample_scores.sum()
                raise ArithmeticError("There is a bug here — rewrite, sample_scores are not sortered yet")
                if scores_sum > 0:
                    sample_scores = sample_scores[-1] * sample_scores / scores_sum

                for i in range(sample_scores.shape[0]):
                    prediction["candidates"].append({
                        "screen_name": sample["candidates"][i]["profile"]["screenName"],
                        "confidence": float(sample_scores[i])
                    })
                prediction["candidates"].sort(key=lambda cand: cand["confidence"], reverse=True)
                prediction["candidates"] = prediction["candidates"][:10]

                top_2 = np.argsort(sample_scores)[-2::][::-1].tolist()

                predicted_id = top_2[0]
                highest_score = sample_scores[top_2[0]]

                if len(top_2) > 1:
                    second_best = sample_scores[top_2[1]]

                if highest_score - second_best > 0.1 and highest_score > 0.20:
                    predicted += 1
                    if predicted < 30:
                        print("  ", sample["entry"]["resourceId"], sample["candidates"][predicted_id]["profile"]["screenName"], str(sample_scores[predicted_id]))
                    prediction["prediction"] = {
                        "screen_name": sample["candidates"][predicted_id]["profile"]["screenName"],
                        "confidence": float(sample_scores[predicted_id])
                    }
                debug_writer.write(json.dumps(prediction)+"\n")

            if counter % check_interval == 0:
                print(" ", "%d samples processed (%.2fs, predictions: %d)" % (counter, (time.time() - timestamp), predicted))

        debug_writer.close()
        print(" ", "%d samples processed (%.2fs, predictions: %d)" % (counter, (time.time() - timestamp), predicted))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple API that returns predictions from a model')
    parser.add_argument('--workdir', required=True, help='Folder with the pipeline result and pretrained models', metavar='#')
    args = parser.parse_args()

    print("Initialized with settings:")
    print(vars(args))

    main(args.workdir)
