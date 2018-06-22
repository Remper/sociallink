import argparse, os, time, json
import numpy as np

from os import path

from evaluation.common import precision_recall_curve
from pairwise_models import restore_definition
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

    print("\nDeserialising test splits")
    test_ids = {}
    test_stats = {}
    for split_id, split in enumerate(splits):
        # Deserializing test sets remembering from which split they came from
        with open(path.join(split, "test.csv"), 'r') as reader:
            ids = []
            test_stats[split_id] = 0
            for line in reader:
                ids.append(line.rstrip().split(',')[0])
            for entity_id in ids[1:]:
                test_ids[entity_id] = split_id
                test_stats[split_id] += 1
    print("Loaded %d test items with following counts: [%s]" % (len(test_ids), ", ".join([str(stat) for stat in test_stats.values()])))

    print("\nDeserialising scalers")
    test_scalers = []
    for split_id, split in enumerate(splits):
        with open(path.join(split, "scaler.json"), 'r') as scaler_reader:
            test_scalers.append(Scaler.from_dict(json.load(scaler_reader)))

    print("\nEvaluation:")
    for model_name in sorted(models.keys()):
        print("Evaluating model %s" % model_name)
        debug_writer = open(path.join(evaluation_dir, model_name + ".dump"), 'w')

        # Invoking a trained model from disk for each split
        model_instances = []
        model_type, feature_set = models[model_name]
        for split_id, split in enumerate(splits):
            model_location = path.join(split, "models", model_type, feature_set, "model")
            model = restore_definition(model_location)
            model.restore_from_file(model_location)
            model_instances.append(model)

        expected = []
        predicted = {}
        for i in np.arange(0.0, 0.5, 0.1):
            predicted[i] = []
        scores = []
        counter = 0
        check_interval = 1000
        timestamp = time.time()
        for sample in test_set:
            counter += 1
            highest_score = -1.0
            predicted_id = -1
            candidate_id = -1
            correct_id = -1
            second_best = -1.0
            sample_features = None
            for features in sample["features"]:
                candidate_id += 1
                if sample_features is None:
                    sample_features = dict()
                    for subspace in features:
                        sample_features[subspace] = []
                for subspace in features:
                    cur_vector = test_scalers[test_ids[sample["entry"]["resourceId"]]].fit_subspace(features[subspace], subspace)
                    sample_features[subspace].append(cur_vector)

                is_current_correct = sample["entry"]["twitterId"].casefold() == sample["candidates"][candidate_id]["profile"]["screenName"].casefold()
                if is_current_correct:
                    if correct_id >= 0:
                        print(" ", "Duplicate correct candidate found")
                    else:
                        correct_id = candidate_id

            debug_writer.write("Entry: %s\n" % sample["entry"]["resourceId"])
            debug_writer.write("Query: -\n")
            if len(sample["features"]) > 0:
                for subspace in sample_features:
                    sample_features[subspace] = np.vstack(sample_features[subspace])
                sample_scores = model_instances[test_ids[sample["entry"]["resourceId"]]].predict(features=sample_features)
                for i in range(sample_scores.shape[0]):
                    debug_writer.write("%.6f\t%.6f\t%d\t%d\t%s\t%s\n" % (sample_scores[i][0], sample_scores[i][1],
                                                                       int(correct_id == i), int(i == 0),
                                                                       sample["entry"]["twitterId"],
                                                                       sample["candidates"][i]["profile"]["screenName"]))

                sample_scores = sample_scores[::, 1]
                top_2 = np.argsort(sample_scores)[-2::][::-1].tolist()

                predicted_id = top_2[0]
                highest_score = sample_scores[top_2[0]]

                if len(top_2) > 1:
                    second_best = sample_scores[top_2[1]]

            for threshold in predicted:
                if highest_score - second_best < threshold:
                    predicted[threshold].append(-1)
                else:
                    predicted[threshold].append(predicted_id)

            expected.append(correct_id)
            scores.append(highest_score)

            if counter % check_interval == 0:
                print(" ", "%d samples processed (%.2fs)" % (counter, (time.time() - timestamp)))

        debug_writer.close()

        with open(path.join(evaluation_dir, model_name+".txt"), 'w') as writer:
            writer.write("Selection:\n")
            writer.write("All")
            for threshold in predicted:
                p, r, s = precision_recall_curve(expected, predicted[threshold], scores)
                for i in range(len(p)):
                    writer.write("\nDNN\t%.4f\t%.4f\t%.4f\t%.2f\t%.2f" % (p[i], r[i], f1(p[i], r[i]), threshold, s[i]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple API that returns predictions from a model')
    parser.add_argument('--workdir', required=True, help='Folder with the pipeline result and pretrained models', metavar='#')
    args = parser.parse_args()

    print("Initialized with settings:")
    print(vars(args))

    main(args.workdir)
