#!/usr/bin/env python3

import sys
from os import path

import logging
import argparse
import os
import pandas as pd
import numpy as np
from random import shuffle

_module = sys.modules['__main__'].__file__
_logger = logging.getLogger(_module)


def candidate_table(cand_path, gold_path):
    cache_file = path.join(args.input, "evaluation_candidates.tsv")
    if os.path.isfile(cache_file):
        return pd.read_csv(cache_file, sep="\t")

    gold = pd.read_csv(gold_path, sep="\t")
    entity_types = {}
    for _, row in gold.iterrows():
        entity = row["?entity"]
        entity_type = row["?type"]
        entity_types[entity] = entity_type

    class Candidate:
        def __init__(self, sn, index, correct):
            self.sn = str(sn)
            self.index = int(index)
            self.correct = int(correct)
            self.scores = dict()  # indexed by method

    class Entity:
        def __init__(self, uri, entity_type):
            self.uri = str(uri)
            self.type = str(entity_type)
            self.correct = "-"
            self.candidates = dict()  # indexed by list index

    def collect(path, entities):
        if (os.path.isdir(path)):
            for dir, _, files in os.walk(path):
                for file in files:
                    collect(os.path.join(dir, file), entities)

        elif (os.path.isfile(path)):
            dir, file = os.path.split(path)
            if (not file.endswith(".dump")):
                return
            method = file[0:file.index(".dump")]
            _logger.info("Processing %s (method: %s)" % (path, method))
            with open(path) as input:
                for line in input:
                    line = line.rstrip()
                    tokens = line.split("\t")
                    if (line.startswith("Entry: ")):
                        uri = line[7:]
                        index = 0
                        entity = entities.get(uri, None)
                        if (entity is None):
                            entity_type = entity_types[uri]
                            entity = Entity(uri, entity_type)
                            entities[uri] = entity
                    elif (len(tokens) == 6):
                        candidate = entity.candidates.get(index, None)
                        entity.correct = tokens[4]
                        if (candidate is None):
                            candidate = Candidate(tokens[5], index, int(tokens[2]))
                            entity.candidates[index] = candidate
                        candidate.scores[method] = float(tokens[1])
                        index += 1

    _logger.info("Recursively scanning files in %s" % cand_path)
    entities = dict()
    collect(cand_path, entities)

    num_entities = len(entities)
    num_candidates = 0
    methods = set()
    for entity in entities.values():
        num_candidates += len(entity.candidates)
        for candidate in entity.candidates.values():
            methods |= set(candidate.scores.keys())
    num_rows = num_entities + num_candidates

    df = pd.DataFrame(index=range(0, num_rows))
    df['uri'] = np.full(num_rows, "-", dtype=str)
    df['type'] = np.full(num_rows, "-", dtype=str)
    df['candidate'] = np.full(num_rows, "-", dtype=str)
    df['index'] = np.full(num_rows, -1, dtype=int)
    df['correct'] = np.full(num_rows, 0, dtype=int)
    for method in sorted(methods):
        df["score_" + method] = np.full(num_rows, 0.0, dtype=float)
    _logger.info("Found %d entities, %d candidates, %d methods", num_entities, num_candidates, len(methods))

    row = 0
    for uri in sorted(entities.keys()):
        entity = entities[uri]
        df.at[row, 'uri'] = entity.uri
        df.at[row, 'type'] = entity.type
        df.at[row, "correct"] = 1
        df.at[row, "candidate"] = entity.correct
        for candidate in entity.candidates.values():
            if (candidate.correct == 1):
                df.at[row, "correct"] = 0
                break
        row += 1
        for index in sorted(entity.candidates.keys()):
            candidate = entity.candidates[index]
            df.at[row, 'uri'] = entity.uri
            df.at[row, 'type'] = entity.type
            df.at[row, 'candidate'] = candidate.sn
            df.at[row, 'index'] = candidate.index
            df.at[row, 'correct'] = candidate.correct
            for method in methods:
                df.at[row, "score_" + method] = candidate.scores.get(method, 0.0)
            row += 1

    df.to_csv(cache_file, sep="\t", index=False)
    _logger.info("Written file %s" % cache_file)

    return df


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--gold", required=True, help="the CSV file containing the gold standard")
    parser.add_argument("--input", required=True, help="the file or directory to collect data from")
    parser.add_argument("--debug", help="log debug messages", action="store_true")
    args = parser.parse_args()

    candidates = candidate_table(args.input, args.gold)
    grouped = candidates.groupby("uri")

    row = 0
    failures = [[], []]
    abstentions = [[], []]
    total_entities = [0, 0]
    total_mistakes = [0, 0]
    total_cand_acquisition_recall_loss = [0, 0]
    total_abstain_with_right = [0, 0]
    total_rest = [0, 0]
    for uri, group in grouped:
        num_correct = np.sum(group["correct"])
        if num_correct > 1:
            _logger.warning("There are %d correct candidates for entity %s" % (num_correct, uri))
        correct = group.at[np.argmax(group["correct"]), "index"]

        row += 1
        for column in candidates.columns:
            if not column.startswith("score_"):
                continue
            method = column[6:]

            scorecol = "score_" + method
            indexcol = "index_" + method

            global_index = np.argmax(group[scorecol])
            index = group.at[global_index, "index"]
            scores = np.sort(group[scorecol].values)

            scores_sum = scores.sum()
            unscaled_top = scores[-1]
            if scores_sum > 0:
                scores = unscaled_top * scores / scores_sum

            score = scores[-1]
            # Mistakes
            type_idx = 0 if group.at[global_index, "type"] == "per" else 1
            total_entities[type_idx] += 1
            if index == correct and correct == -1:
                total_mistakes[type_idx] += 1
                total_cand_acquisition_recall_loss[type_idx] += 1
            if index != correct:
                total_mistakes[type_idx] += 1
                if correct != -1:
                    recorded_candidates = []
                    for element in group.values:
                        recorded_candidates.append([element[3], element[2], element[5], unscaled_top * element[5] / scores_sum])
                    failure = {"method": method, "uri": uri, "correct": correct, "win": index, "candidates": recorded_candidates}
                    if group.at[global_index, "type"] == "org":
                        if score > 0.138:
                            failures[type_idx].append(failure)
                            total_rest[type_idx] += 1
                        else:
                            total_abstain_with_right[type_idx] += 1
                            abstentions[type_idx].append(failure)
                    else:
                        if score > 0.197:
                            failures[type_idx].append(failure)
                            total_rest[type_idx] += 1
                        else:
                            total_abstain_with_right[type_idx] += 1
                            abstentions[type_idx].append(failure)
                else:
                    total_cand_acquisition_recall_loss[type_idx] += 1

        if row % 10000 == 0:
            print("Processed %2dk rows (total mistakes: %4d, total entities: %d)" % (row / 1000, sum(total_mistakes), sum(total_entities)))


    def percent(value, denom):
        return (value / denom) * 100

    def numbers(value, denom, denom2):
        return sum(value), percent(sum(value), sum(denom)), value[0], value[1], percent(sum(value), sum(denom2))

    print()
    print("Total: %d\n  Persons: %d\n  Organisations: %d" % (sum(total_entities), total_entities[0], total_entities[1]))
    print("Total mistakes: %4d (%.2f%%, per: %d, org: %d, percentage from total: %.2f%%)" % numbers(total_mistakes, total_entities, total_entities))
    print("Cand. acquisition recall loss: %4d (%.2f%%, per: %d, org: %d, percentage from total: %.2f%%)" % numbers(total_cand_acquisition_recall_loss, total_mistakes, total_entities))
    print("Abstain with right: %4d (%.2f%%, per: %d, org: %d, percentage from total: %.2f%%)" % numbers(total_abstain_with_right, total_mistakes, total_entities))
    print("Rest: %4d (%.2f%%, per: %d, org: %d, percentage from total: %.2f%%)" % numbers(total_rest, total_mistakes, total_entities))

    shuffle(failures[0])
    shuffle(failures[1])
    shuffle(abstentions[0])
    shuffle(abstentions[1])

    def print_failure(failure):
        print()
        print("URI:\t%s\t\t\t\t\tMethod: %s\nCorrect index: #%d\tChosen: #%d" % (
            failure["uri"], failure["method"], failure["correct"], failure["win"]))
        if len(failure["candidates"]) > 0:
            print("  #id\tScreen Name\t\tRaw Score\t\tScore")
            print("\n".join(
                ["  #%2d\t@%-20s\t\t%.3f\t\t%.3f" % (candidate[0], candidate[1], candidate[2], candidate[3]) for
                 candidate in failure["candidates"]]))

    print()
    print("Abstentions:")
    for element in abstentions[0][:10]+abstentions[1][:10]:
        print_failure(element)

    print()
    print("Persons:")
    for element in failures[0][:20]:
        print_failure(element)

    print()
    print("Organisations:")
    for element in failures[1][:20]:
        print_failure(element)