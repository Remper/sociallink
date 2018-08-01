import argparse, os, time, json
import numpy as np

from os import path

from evaluation.common import precision_recall_curve
from pairwise_models import restore_definition
from rule_based.most_followers import MostFollowers
from utils.common import Scaler


def f1(prec: float, rec: float) -> float:
    return 2 * prec * rec / (prec + rec)


def present(workdir, file):
    if path.exists(path.join(workdir, file)):
        print(" ", "%s exists" % file)
    else:
        raise Exception("%s does not exist" % file)


def main(workdir):
    print("Performing checks")
    [present(workdir, file) for file in ["dataset.json", "splits", "manifest.json"]]

    # Preparing the evaluation directory
    evaluation_dir = path.join(workdir, "evaluation")
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

    print("\nDeserialising dataset")
    timestamp = time.time()
    test_set = []
    counter = 0
    with open(path.join(workdir, "dataset.json"), 'r') as reader:
        for line in reader:
            sample = json.loads(line)
            del sample["resource"]
            test_set.append(sample)
            counter += 1
            if counter % 5000 == 0:
                print("  Loaded %d samples" % counter)
    print("Done in %.2fs, loaded %d samples" % (time.time() - timestamp, len(test_set)))

    print("\nDeserialising scalers")
    test_scalers = []
    for split_id, split in enumerate(splits):
        with open(path.join(split, "scaler.json"), 'r') as scaler_reader:
            test_scalers.append(Scaler.from_dict(json.load(scaler_reader)))

    print("\nEvaluation:")
    for model_name in sorted(models.keys()):
        print("Evaluating model %s" % model_name)

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
        for sample in test_set:
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

            prediction = {
                "entry": sample["entry"]["resourceId"],
                "candidates": [],
                "prediction": None
            }
            if len(sample["features"]) > 0:
                for subspace in sample_features:
                    sample_features[subspace] = np.vstack(sample_features[subspace])
                sample_scores = model_instances[0].predict(features=sample_features)
                for i in range(sample_scores.shape[0]):
                    prediction["candidates"].append({
                        "screen_name": sample["candidates"][i]["profile"]["screenName"],
                        "confidence": float(sample_scores[i][1])
                    })

                sample_scores = sample_scores[::, 1]
                top_2 = np.argsort(sample_scores)[-2::][::-1].tolist()

                predicted_id = top_2[0]
                highest_score = sample_scores[top_2[0]]

                if len(top_2) > 1:
                    second_best = sample_scores[top_2[1]]

                if highest_score - second_best < 0.1 and highest_score > 0.7:
                    prediction["prediction"] = {
                        "screen_name": sample["candidates"][predicted_id]["profile"]["screenName"],
                        "confidence": float(sample_scores[predicted_id][1])
                    }
            debug_writer.write(json.dumps(prediction)+"\n")

            if counter % check_interval == 0:
                print(" ", "%d samples processed (%.2fs)" % (counter, (time.time() - timestamp)))

        debug_writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple API that returns predictions from a model')
    parser.add_argument('--workdir', required=True, help='Folder with the pipeline result and pretrained models', metavar='#')
    args = parser.parse_args()

    print("Initialized with settings:")
    print(vars(args))

    main(args.workdir)
