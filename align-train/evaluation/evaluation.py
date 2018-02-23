#!/usr/bin/env python3

import sys
import logging
import argparse
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from evaluation.common import precision_recall_curve, draw_f1_lines

_module = sys.modules['__main__'].__file__
_logger = logging.getLogger(_module)


_hack = False
_joint = True
_min_score = 0.0
_min_improvement = 0
_input_dir = "./ngs-en-fold3"
_plot_file_all = "evaluation_plot.pdf"
_plot_file_selection = "evaluation_plot_selection.pdf"
_plot_file_joint = "evaluation_plot_joint.pdf"


def candidate_table(path):

    cache_file = "evaluation_candidates.tsv"
    if (os.path.isfile(cache_file)):
        return pd.read_csv(cache_file, sep="\t")
    
    class Candidate:
        def __init__(self, sn, index, correct):
            self.sn = str(sn)
            self.index = int(index)
            self.correct = int(correct)
            self.scores = dict() # indexed by method
    
    class Entity:
        def __init__(self, uri, entity_type):
            self.uri = str(uri)
            self.type = str(entity_type)
            self.candidates = dict() # indexed by list index

    def collect(path, entities):
        if (os.path.isdir(path)):
            for dir, _, files in os.walk(path):
                for file in files:
                    collect(os.path.join(dir, file), entities)
    
        elif (os.path.isfile(path)):
            dir, file = os.path.split(path)
            if (file == "organisations-selection.txt"):
                entity_type = "org"
            elif (file == "persons-selection.txt"):
                entity_type = "per"
            else:
                return
            method = os.path.split(dir)[1]        
            _logger.info("Processing %s (method: %s, type: %s)" % (path, method, entity_type))        
            with open(path) as input:
                for line in input:
                    line = line.rstrip()
                    tokens = line.split("\t")
                    if (line.startswith("Entry: ")):
                        uri = line[7:]
                        index = 0
                        entity = entities.get(uri, None)
                        if (entity is None):
                            entity = Entity(uri, entity_type)
                            entities[uri] = entity
                    elif (len(tokens) == 9):
                        candidate = entity.candidates.get(index, None)
                        if (candidate is None):
                            candidate = Candidate(tokens[8], index, tokens[2])
                            entity.candidates[index] = candidate
                        candidate.scores[method] = float(tokens[1])
                        candidate.scores["baseline"] = float(tokens[3])
                        index += 1

    _logger.info("Recursively scanning files in %s" % path)    
    entities = dict()
    collect(path, entities)

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


def entity_table (candidates, min_improvement):
    
    cache_file = "evaluation_entities-%.2f.tsv" % min_improvement
    if (os.path.isfile(cache_file)):
        return pd.read_csv(cache_file, sep="\t")
        
    grouped = candidates.groupby("uri")
    
    num_rows = len(grouped)
    methods = set()
    df = pd.DataFrame(index=range(0, num_rows))
    df['uri'] = np.full(num_rows, "-", dtype=str)
    df['type'] = np.full(num_rows, "-", dtype=str)
    df['correct'] = np.full(num_rows, 0, dtype=int)
    for column in candidates.columns:
        if (column.startswith("score_")):
            method = column[6:]
            methods.add(method)
            df["index_" + method] = np.full(num_rows, 0, dtype=int)
            df["score_" + method] = np.full(num_rows, 0.0, dtype=float)
    df['num_candidates'] = np.full(num_rows, 0, dtype=int)
    df['candidates'] = np.full(num_rows, "-", dtype=str)
    
    row = 0
    for uri, group in grouped:
        index = group.index
        correct = group.at[np.argmax(group["correct"]), "index"]
        df.at[row, "uri"] = uri
        df.at[row, "type"] = group.at[index[0], "type"]
        df.at[row, "correct"] = correct
        df.at[row, "correct_joint"] = correct if correct >= 0 else 1000
        df.at[row, "num_candidates"] = len(group["candidate"])
        df.at[row, "candidates"] = " ".join(group["candidate"])
        for method in methods:
            scorecol = "score_" + method
            indexcol = "index_" + method

            if (_hack):
                index = -1
                score = -200.0
                improvement = -0.0
                for r in group.index:
                    i = group.at[r, "index"]
                    s = group.at[r, scorecol]
                    if (i >= 0 and s > score):
                        index = i
                        improvement = s - score
                        score = s
            else:
                index = group.at[np.argmax(group[scorecol]), "index"]
                scores = np.sort(group[scorecol])
                score = scores[-1]
                score_next = scores[-2] if len(scores) > 2 else -1
                improvement = score -score_next
                            
            if (improvement > min_improvement):
                df.at[row, scorecol] = score
                df.at[row, indexcol] = index
            else:
                df.at[row, scorecol] = 1
                df.at[row, indexcol] = -1
        row += 1
    
    if (cache_file):
        df.to_csv(cache_file, sep="\t", index=False) 
        _logger.info("Written file %s" % cache_file)

    return df


def precision_recall_plot(data, expected_col, predicted_cols, score_cols, labels, rrange=(0, 1), prange=(0, 1), legendloc="best", title=None, ax=None, f1lines=True):
    
    if not ax:
        ax = plt.subplot2grid((1, 1), (0, 0))
    
    if f1lines:
        draw_f1_lines(ax, rrange, prange)
        
    for i in range(0, len(labels)):
        label = labels[i]
        predicted_col = predicted_cols[i]
        score_col = score_cols[i]
        p, r, _ = precision_recall_curve(data[expected_col], data[predicted_col], data[score_col])
        ax.plot(r, p, "-" if len(p) > 1 else "s-", label=label)
 
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    #ax.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
    #ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f'))
    ax.set_xlim(rrange)
    ax.set_ylim(prange)
    if (legendloc):
        ax.legend(loc=legendloc)
    if (title):
        ax.set_title(title)

    plt.tight_layout()
    return ax
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", nargs=1, default=_input_dir, help="the file or directory to collect data from")
    parser.add_argument("--debug", help="log debug messages", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(stream=sys.stderr,
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s (%(levelname).1s) %(message)s',
        datefmt='%m-%d %H:%M:%S')
        
    candidates = candidate_table(args.input)
    
    methods = [column[6:] for column in candidates.columns if column.startswith("score_")]
    pcols = ["index_" + method for method in methods]
    scols = ["score_" + method for method in methods]

    entities = entity_table(candidates, 0.4)

    df = entities[entities.num_candidates > 1]
    p, r, s = precision_recall_curve(df["correct"], df["index_emb_extra@full"], df["score_emb_extra@full"])
    pj, rj, sj = precision_recall_curve(entities["correct_joint"], entities["index_emb_extra@full"], entities["score_emb_extra@full"])
   
#    plt.figure(figsize=(8, 6))
#    precision_recall_plot(entities, "correct_joint", pcols, scols, methods, prange=(.7, 1), legendloc="upper right")
#    plt.savefig(_plot_file_joint)    
#
#    plt.figure(figsize=(8, 5))
#    precision_recall_plot(entities[entities.num_candidates > 1], "correct", pcols, scols, methods, prange=(.7, 1), legendloc="upper right")
#    plt.savefig(_plot_file_selection)    

    _min_improvements = np.linspace(0, 0.4, 2)
    _grid = (2 * len(_min_improvements), 6)
    _row = 0
    plt.figure(figsize=(30, 10 * len(_min_improvements)))
    for _min_imp in _min_improvements:
        ents = entity_table(candidates, _min_imp)
        precision_recall_plot(ents[(ents.type == "per") & (ents.num_candidates > 1)], "correct", pcols, scols, methods, title="Selection PER - %.2f" % _min_imp, ax = plt.subplot2grid(_grid, (_row, 0)), legendloc=None)
        precision_recall_plot(ents[(ents.type == "org") & (ents.num_candidates > 1)], "correct", pcols, scols, methods, title="Selection ORG - %.2f" % _min_imp, ax = plt.subplot2grid(_grid, (_row + 1, 0)), legendloc=None)
        precision_recall_plot(ents[ents.num_candidates > 1], "correct", pcols, scols, methods, title="Selection ALL - %.2f" % _min_imp, ax = plt.subplot2grid(_grid, (_row, 1), rowspan=2, colspan=2))
        precision_recall_plot(ents, "correct_joint", pcols, scols, methods, title="Joint ALL - %.2f" % _min_imp, ax = plt.subplot2grid(_grid, (_row, 3), rowspan=2, colspan=2))
        precision_recall_plot(ents[ents.type == "per"], "correct_joint", pcols, scols, methods, title="Joint PER - %.2f" % _min_imp, ax = plt.subplot2grid(_grid, (_row, 5)), legendloc=None)
        precision_recall_plot(ents[ents.type == "org"], "correct_joint", pcols, scols, methods, title="Joint ORG - %.2f" % _min_imp, ax = plt.subplot2grid(_grid, (_row + 1, 5)), legendloc=None)
        _row += 2
    plt.savefig(_plot_file_all)
        
