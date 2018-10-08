#!/usr/bin/env python3

import sys
from os import path

import logging
import argparse
import warnings
import os
import time
import pandas as pd
import numpy as np
import numexpr as ne
import matplotlib.pyplot as plt

from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss

_module = sys.modules['__main__'].__file__
_logger = logging.getLogger(_module)

_hack = False
_joint = True
_min_score = 0.0
_min_improvement = 0
_input_dir = "./ngs-en-final-filtered"
_gold_file = "./gold_en.tsv"
_plot_file_all = "evaluation_plot.pdf"
_plot_file_selection = "evaluation_plot_selection.pdf"
_plot_file_joint = "evaluation_plot_joint.pdf"


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
                        if (candidate is None):
                            candidate = Candidate(tokens[5], index, int(tokens[2]))
                            entity.candidates[index] = candidate
                        candidate.scores[method] = float(tokens[1])
                        candidate.scores["baseline"] = float(tokens[3])
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


def entity_table(candidates, min_improvement):
    cache_file = path.join(args.input, "evaluation_entities-%.2f.tsv" % min_improvement)
    if (os.path.isfile(cache_file)):
        return pd.read_csv(cache_file, sep="\t")

    grouped = candidates.groupby("uri")

    num_rows = len(grouped)
    methods = set()
    df = pd.DataFrame(index=range(0, num_rows))
    df['uri'] = np.full(num_rows, "-", dtype=str)
    df['type'] = np.full(num_rows, "-", dtype=str)
    df['correct'] = np.full(num_rows, 0, dtype=int)
    df["index_ref"] = np.full(num_rows, 0, dtype=int)
    df["score_ref"] = np.full(num_rows, 0.0, dtype=float)
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
        num_correct = np.sum(group["correct"])
        if (num_correct > 1):
            _logger.warn("There are %d correct candidates for entity %s" % (num_correct, uri))
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
                scores = np.sort(group[scorecol].values)
                # scores = np.sort(np.exp(group[scorecol].values))

                if (method == "simple@iswc17"):
                    score = scores[-1]
                    score_next = scores[-2] if len(scores) > 2 else 0
                    improvement = score - score_next
                    if (improvement >= min_improvement):
                        df.at[row, "score_ref"] = score
                        df.at[row, "index_ref"] = index
                    else:
                        df.at[row, "score_ref"] = 1
                        df.at[row, "index_ref"] = -1

                scores_sum = scores.sum()
                if (scores_sum > 0):
                    scores = scores[-1] * scores / scores_sum

                    # scores_sum = scores.sum()
                score = scores[-1]
                score_next = scores[-2] if len(scores) > 2 else 0  # TODO -1
                improvement = score - score_next

            if (improvement >= min_improvement):
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


def pr_measures(expected, predicted):
    n = len(expected)
    assert n == len(predicted)

    different = expected != predicted
    not_nil = expected >= 0
    not_abstain = predicted >= 0

    fp = (different & not_abstain).sum()
    fn = (different & not_nil).sum()
    tp = not_nil.sum() - fn

    #    tp = np.logical_and(expected == predicted, expected >= 0).sum()
    #    fp = np.logical_and(expected != predicted, predicted >= 0).sum()
    #    fn = np.logical_and(expected != predicted, expected >= 0).sum()
    #    tn = np.logical_and(expected == predicted, expected < 0).sum()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        p = tp / (tp + fp)
        r = tp / (tp + fn)
        f1 = 2 * p * r / (p + r)

    return np.array([p, r, f1])


def pr_fscore(p, r, beta=1):
    beta2 = beta * beta
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return np.nan_to_num((1 + beta2) * p * r / (beta2 * p + r))


def pr_approximate_randomization(expected, predicted1, predicted2, iterations=1000):
    n = len(expected)
    assert n == len(predicted1)
    assert n == len(predicted2)

    def compare(e, p):
        tp = ((e == p) & (e >= 0)).astype(int)
        fp = ((e != p) & (p >= 0)).astype(int)
        fn = ((e != p) & (e >= 0)).astype(int)
        return tp, fp, fn

    def test_statistics(tp1, fp1, fn1, tp2, fp2, fn2):
        tp1c = tp12c + tp1.sum()
        tp2c = tp12c + tp2.sum()
        fp1c = fp12c + fp1.sum()
        fp2c = fp12c + fp2.sum()
        fn1c = fn12c + fn1.sum()
        fn2c = fn12c + fn2.sum()
        if (tp1c == 0):
            p1 = r1 = f1 = 0
        else:
            p1 = tp1c / (tp1c + fp1c)
            r1 = tp1c / (tp1c + fn1c)
            f1 = 2 * p1 * r1 / (p1 + r1)
        if (tp2c == 0):
            p2 = r2 = f2 = 0
        else:
            p2 = tp2c / (tp2c + fp2c)
            r2 = tp2c / (tp2c + fn2c)
            f2 = 2 * p2 * r2 / (p2 + r2)
        return np.abs(np.array([p1 - p2, r1 - r2, f1 - f2]))

    e = np.array(expected)
    p1 = np.array(predicted1)
    p2 = np.array(predicted2)

    sp = (p1 == p2) | ((p1 != e) & (p2 != e) & (p1 >= 0) & (p2 >= 0))
    dp = ~sp
    de = e[dp]
    m = len(de)

    tp12, fp12, fn12 = compare(e[sp], p1[sp])
    tp12c, fp12c, fn12c = tp12.sum(), fp12.sum(), fn12.sum()

    tp1, fp1, fn1 = compare(de, p1[dp])
    tp2, fp2, fn2 = compare(de, p2[dp])

    tref = test_statistics(tp1, fp1, fn1, tp2, fp2, fn2)

    tp1c = tp12c + tp1.sum()
    tp2c = tp12c + tp2.sum()
    fp1c = fp12c + fp1.sum()
    fp2c = fp12c + fp2.sum()
    fn1c = fn12c + fn1.sum()
    fn2c = fn12c + fn2.sum()

    tpd = tp1 - tp2
    fpd = fp1 - fp2
    fnd = fn1 - fn2

    np.random.seed(n)
    r = np.zeros(len(tref))

    for i in range(0, iterations):
        mask = np.random.randint(low=0, high=2, size=m)
        tpdm = np.inner(tpd, mask)
        fpdm = np.inner(fpd, mask)
        fndm = np.inner(fnd, mask)

        tp3c = tp1c - tpdm
        tp4c = tp2c + tpdm
        fp3c = fp1c - fpdm
        fp4c = fp2c + fpdm
        fn3c = fn1c - fndm
        fn4c = fn2c + fndm

        if (tp3c == 0):
            p3 = r3 = f3 = 0
        else:
            p3 = tp3c / (tp3c + fp3c)
            r3 = tp3c / (tp3c + fn3c)
            f3 = 2 * p3 * r3 / (p3 + r3)
        if (tp4c == 0):
            p4 = r4 = f4 = 0
        else:
            p4 = tp4c / (tp4c + fp4c)
            r4 = tp4c / (tp4c + fn4c)
            f4 = 2 * p4 * r4 / (p4 + r4)
        t = np.abs(np.array([p3 - p4, r3 - r4, f3 - f4]))

        #        mask = np.random.randint(low=0, high=2, size=m)
        #        tpdm = tpd * mask; tp3 = tpdm + tp2; tp4 = tp1 - tpdm
        #        fpdm = fpd * mask; fp3 = fpdm + fp2; fp4 = fp1 - fpdm
        #        fndm = fnd * mask; fn3 = fndm + fn2; fn4 = fn1 - fndm
        #        t = test_statistics(tp3, fp3, fn3, tp4, fp4, fn4)
        r += t >= tref

    return (r + 1) / (iterations + 1)


def compute_tp_fp_fn(expected, predicted):
    e = np.asarray(expected)
    p = np.asarray(predicted)

    n = len(e)
    assert n == len(p)

    # handle non-numeric input
    r = np.zeros(())

    tp = ((e == p) & (e >= 0)).astype(int)
    fp = ((e != p) & (p >= 0)).astype(int)
    fn = ((e != p) & (e >= 0)).astype(int)


def approximate_randomization(sample1, sample2, aggregate_function=None, iterations=1000):
    n = len(expected)
    assert n == len(predicted1)
    assert n == len(predicted2)

    def compare(e, p):
        tp = ((e == p) & (e >= 0)).astype(int)
        fp = ((e != p) & (p >= 0)).astype(int)
        fn = ((e != p) & (e >= 0)).astype(int)
        return tp, fp, fn

    def test_statistics(tp1, fp1, fn1, tp2, fp2, fn2):
        tp1c = tp12c + tp1.sum()
        tp2c = tp12c + tp2.sum()
        fp1c = fp12c + fp1.sum()
        fp2c = fp12c + fp2.sum()
        fn1c = fn12c + fn1.sum()
        fn2c = fn12c + fn2.sum()
        if (tp1c == 0):
            p1 = r1 = f1 = 0
        else:
            p1 = tp1c / (tp1c + fp1c)
            r1 = tp1c / (tp1c + fn1c)
            f1 = 2 * p1 * r1 / (p1 + r1)
        if (tp2c == 0):
            p2 = r2 = f2 = 0
        else:
            p2 = tp2c / (tp2c + fp2c)
            r2 = tp2c / (tp2c + fn2c)
            f2 = 2 * p2 * r2 / (p2 + r2)
        return np.abs(np.array([p1 - p2, r1 - r2, f1 - f2]))

    e = np.array(expected)
    p1 = np.array(predicted1)
    p2 = np.array(predicted2)

    sp = (p1 == p2) | ((p1 != e) & (p2 != e) & (p1 >= 0) & (p2 >= 0))
    dp = ~sp
    de = e[dp]
    m = len(de)

    tp12, fp12, fn12 = compare(e[sp], p1[sp])
    tp12c, fp12c, fn12c = tp12.sum(), fp12.sum(), fn12.sum()

    tp1, fp1, fn1 = compare(de, p1[dp])
    tp2, fp2, fn2 = compare(de, p2[dp])

    tref = test_statistics(tp1, fp1, fn1, tp2, fp2, fn2)

    tp1c = tp12c + tp1.sum()
    tp2c = tp12c + tp2.sum()
    fp1c = fp12c + fp1.sum()
    fp2c = fp12c + fp2.sum()
    fn1c = fn12c + fn1.sum()
    fn2c = fn12c + fn2.sum()

    tpd = tp1 - tp2
    fpd = fp1 - fp2
    fnd = fn1 - fn2

    np.random.seed(n)
    r = np.zeros(len(tref))

    for i in range(0, iterations):
        mask = np.random.randint(low=0, high=2, size=m)
        tpdm = np.inner(tpd, mask)
        fpdm = np.inner(fpd, mask)
        fndm = np.inner(fnd, mask)

        tp3c = tp1c - tpdm
        tp4c = tp2c + tpdm
        fp3c = fp1c - fpdm
        fp4c = fp2c + fpdm
        fn3c = fn1c - fndm
        fn4c = fn2c + fndm

        if (tp3c == 0):
            p3 = r3 = f3 = 0
        else:
            p3 = tp3c / (tp3c + fp3c)
            r3 = tp3c / (tp3c + fn3c)
            f3 = 2 * p3 * r3 / (p3 + r3)
        if (tp4c == 0):
            p4 = r4 = f4 = 0
        else:
            p4 = tp4c / (tp4c + fp4c)
            r4 = tp4c / (tp4c + fn4c)
            f4 = 2 * p4 * r4 / (p4 + r4)
        t = np.abs(np.array([p3 - p4, r3 - r4, f3 - f4]))

        #        mask = np.random.randint(low=0, high=2, size=m)
        #        tpdm = tpd * mask; tp3 = tpdm + tp2; tp4 = tp1 - tpdm
        #        fpdm = fpd * mask; fp3 = fpdm + fp2; fp4 = fp1 - fpdm
        #        fndm = fnd * mask; fn3 = fndm + fn2; fn4 = fn1 - fndm
        #        t = test_statistics(tp3, fp3, fn3, tp4, fp4, fn4)
        r += t >= tref

    return (r + 1) / (iterations + 1)


def bootstrap_confidence_intervals(expected, predicted, measure_function, confidence=95, iterations=1000):
    n = len(expected)
    assert n == len(predicted)

    expected = np.asarray(expected)
    predicted = np.asarray(predicted)

    mref = measure_function(expected, predicted)
    m = len(mref)

    np.random.seed(n)

    msampled = np.zeros((iterations, m))
    for i in range(0, iterations):
        indexes = np.random.randint(low=0, high=n, size=n)
        e = expected[indexes]
        p = predicted[indexes]
        msampled[i, :] = measure_function(e, p)

    lower_percentile = (100 - confidence) / 2
    upper_percentile = 100 - lower_percentile

    lower_margin = mref - np.percentile(msampled, lower_percentile, axis=0)
    upper_margin = np.percentile(msampled, upper_percentile, axis=0) - mref
    margin = np.max(np.stack((lower_margin, upper_margin)), axis=0)

    return np.stack((mref, margin, lower_margin, upper_margin))


def approximate_randomization_old(expected, predicted_baseline, predicted_system, measure_function, twosided=True,
                                  iterations=1000):
    n = len(expected)
    assert n == len(predicted_baseline)
    assert n == len(predicted_system)

    expected = np.array(expected)
    predicted_baseline = np.array(predicted_baseline)
    predicted_system = np.array(predicted_system)

    mb = measure_function(expected, predicted_baseline)
    ms = measure_function(expected, predicted_system)
    tref = np.abs(ms - mb)

    np.random.seed(n)
    r = np.zeros(len(tref))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for i in range(0, iterations):
            indexes = np.random.randint(low=0, high=2, size=n)
            temp = (predicted_baseline - predicted_system) * indexes
            predicted1 = temp + predicted_system
            predicted2 = predicted_baseline - temp
            m1 = measure_function(expected, predicted1)
            m2 = measure_function(expected, predicted2)
            t = np.abs(m1 - m2)
            r += t >= tref

    pvalues = (r + 1) / (iterations + 1)
    if (not twosided):
        pvalues = pvalues / 2

    return pvalues


def plot_approximate_randomization(entities, baseline_method, joint=True):
    expected_col = "correct_joint" if joint else "correct"
    if (not joint):
        entities = entities[entities.num_candidates > 1]

    methods = [column[6:] for column in candidates.columns if column.startswith("score_")]

    expected = entities[expected_col]

    predicted_by_method = dict()
    for method in methods:
        predicted = entities["index_" + method]
        scores = entities["score_" + method]
        p, r, s = precision_recall_curve(expected, predicted, scores)
        f1 = pr_fscore(p, r)
        threshold = s[np.argmax(f1)]
        predicted = predicted.copy()
        predicted[scores < threshold] = -1
        predicted_by_method[method] = predicted

    for method in methods:
        predicted_baseline = predicted_by_method[baseline_method]
        predicted_method = predicted_by_method[method]
        #        expected.to_csv("ar_expected_" + method + ".tsv", sep="\t")
        #        predicted_baseline.to_csv("ar_baseline_" + method + ".tsv", sep="\t")
        #        predicted_method.to_csv("ar_method_" + method + ".tsv", sep="\t")
        t1 = time.process_time()
        # pvalues = approximate_randomization_old(expected, predicted_baseline, predicted_method, pr_measures, iterations=100000)
        pvalues = pr_approximate_randomization(expected, predicted_baseline, predicted_method, iterations=100000)
        t2 = time.process_time()
        m = pr_measures(expected, predicted_method)
        print("method: %-25s - p=%.6f r=%.6f f1=%.6f - pvalues: p=%.6f r=%.6f f1=%.6f - %f sec" % (
        method, m[0], m[1], m[2], pvalues[0], pvalues[1], pvalues[2], t2 - t1))


def precision_recall_curve(expected, predicted, scores):
    # Sort predictions by decreasing score
    desc_score_indices = np.argsort(scores, kind="mergesort")[::-1]
    expected = np.array(expected)[desc_score_indices]
    predicted = np.array(predicted)[desc_score_indices]
    scores = np.array(scores)[desc_score_indices]

    # Classify each prediction as TP, FP, FN
    tp = np.logical_and(expected == predicted, expected >= 0)
    fp = np.logical_and(expected != predicted, predicted >= 0)
    fn = np.logical_and(expected != predicted, expected >= 0)

    # Aggregate TP, FP, FN by decreasing score threshold
    tpc = np.cumsum(tp)
    fpc = np.cumsum(fp)
    fnc = np.cumsum(fn) + np.append(np.cumsum((expected >= 0)[::-1])[-2::-1], 0)

    # Keep only unique score thresholds, and filter scores, tpc, fpc, fnc accordingly
    thresholds = np.where(np.diff(np.append(scores, -1)))[0]
    scores = scores[thresholds]
    tpc = tpc[thresholds]
    fpc = fpc[thresholds]
    fnc = fnc[thresholds]

    # Compute precision and recall
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        p = tpc / (tpc + fpc)
        r = tpc / (tpc + fnc)

    # Print thresholds and p/r for p >= 90%
    high_p_idx = p >= 0.9
    high_p = p[high_p_idx]
    high_r = r[high_p_idx]
    high_score = scores[high_p_idx]

    #print("Thresholds:")
    print("P:        ", high_p[-5:])
    print("R:        ", high_r[-5:])
    print("Threshold:", high_score[-5:])
    print()

    # Discard NAN precision values and (precision, recall) pairs after recall reaches 1
    idx = ~np.isnan(p) & ~np.isnan(r) & (np.diff(np.append(r, 2)) > 0)
    scores = scores[idx]
    p = np.nan_to_num(p[idx])
    r = np.nan_to_num(r[idx])

    # Return result
    return (p, r, scores)


def precision_recall_plot(data, expected_col, predicted_cols, score_cols, labels, rrange=(0, 1), prange=(0, 1),
                          legendloc="best", title=None, ax=None, f1lines=True):
    if (not ax):
        ax = plt.subplot2grid((1, 1), (0, 0))

    if (f1lines):
        for f in np.linspace(0.1, .9, 9):
            # estimate p, r curve for points with F1 = f
            alpha = np.linspace(f / (2 - f), (2 - f) / f, 100)
            p = f * (alpha + 1) / 2
            r = p / alpha

            # estimate coeffs of P = pa * R + pb for diagonal
            pa = (prange[1] - prange[0]) / (rrange[1] - rrange[0])
            pb = prange[0] - rrange[0] * pa

            # estimate coeffs of aR^2 + bR + c = 0
            a = 2 * pa
            b = 2 * pb - f * (pa + 1)
            c = -f * pb

            # estimate location where to put text
            rt = 1 / (2 * a) * (-b + (b ** 2 - 4 * a * c) ** .5)
            pt = pa * rt + pb

            # plot lines and labels
            ax.plot(r, p, "#e8e8e8")
            ax.text(rt, pt, "$F_1 = %.1f$" % f, color="#aaaaaa", size="x-small")

    for i in range(0, len(labels)):
        label = labels[i]
        predicted_col = predicted_cols[i]
        score_col = score_cols[i]
        p, r, _ = precision_recall_curve(data[expected_col], data[predicted_col], data[score_col])
        ax.plot(r, p, "-" if len(p) > 1 else "s-", label=label)

    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    # ax.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
    # ax.xaxis.set_major_formatter(FormatStrFormatter('%.2f'))
    ax.set_xlim(rrange)
    ax.set_ylim(prange)
    if (legendloc):
        ax.legend(loc=legendloc)
    if (title):
        ax.set_title(title)

    plt.tight_layout()
    return ax


def plot_candidate_recall(entities, ax=None):
    m = max(entities["correct"]) + 1

    def compute_counts(ents):
        n = len(ents)
        correct_indexes = ents["correct"]
        idx, idx_counts = np.unique(correct_indexes[correct_indexes >= 0], return_counts=True)
        counts = np.zeros(m)
        counts[idx] = idx_counts
        return np.cumsum(counts) / n

    counts_all = compute_counts(entities)
    #    counts_per = compute_counts(entities[entities.type == "per"])
    #    counts_org = compute_counts(entities[entities.type == "org"])

    if (not ax):
        ax = plt.subplot2grid((1, 1), (0, 0))

    x = range(1, m + 1)
    ax.plot(x, counts_all, "-", label="all entities")
    #    ax.plot(x, counts_per, "-", label="persons")
    #    ax.plot(x, counts_org, "-", label="organisations")
    ax.set_xlabel("Threshold $k$")
    ax.set_ylabel("Recall")
    plt.tight_layout()
    return ax


def plot_candidate_histogram(entities, ax=None):
    if (not ax):
        ax = plt.subplot2grid((1, 1), (0, 0))

    m = max(entities["correct"]) + 1
    num_candidates = entities["num_candidates"] - 1

    ax.hist(num_candidates, bins=m + 1, histtype="step", density=True)
    ax.set_xlabel("# Candidates per entity")
    ax.set_ylabel("Frequency")
    plt.tight_layout()
    return ax


def plot_calibration(entities, method, bins=100):
    ents = entities[entities.correct >= 0]

    y_true = np.asarray(ents.correct == ents["index_" + method]) + 0
    y_prob = np.asarray(ents["score_" + method])
    y, x = calibration_curve(y_true, y_prob, n_bins=bins)
    brier = brier_score_loss(y_true, y_prob)
    _logger.info("Brier score %s: %f" % (method, brier))

    plt.figure(1, figsize=(10, 10))
    ax1 = plt.subplot2grid((3, 1), (0, 0), rowspan=2)
    ax2 = plt.subplot2grid((3, 1), (2, 0))

    ax1.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")
    ax1.plot(x, y, "s-", label=method)
    ax1.set_ylabel("Actual probability")
    ax1.set_ylim([-0.05, 1.05])
    ax1.legend(loc="upper left")
    ax1.set_title('Calibration plots')

    ax2.hist(y_prob[method], range=(0, 1), bins=100, histtype="step", density=True)
    ax2.set_xlabel("Estimated probability")
    ax2.set_ylabel("Frequency")

    plt.tight_layout()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--gold", default=_gold_file, help="the CSV file containing the gold standard")
    parser.add_argument("--input", default=_input_dir, help="the file or directory to collect data from")
    parser.add_argument("--debug", help="log debug messages", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr,
                        level=logging.DEBUG if args.debug else logging.INFO,
                        format='%(asctime)s (%(levelname).1s) %(message)s',
                        datefmt='%m-%d %H:%M:%S')

    candidates = candidate_table(args.input, args.gold)

    method_labels = {
        "baseline": "MOST_POPULAR",
        "ref": "ISWC2017",
        "emb_extra_layer@iswc17+emb_kb200_rdf2vec_w+emb_sg300_w": "BASE_KB_SG_TL",
        "all": "ALL",
        "glove": "GloVe",
        "fasttext": "fastText",
        "lsa": "LSA",
        "emb_extra_layer@iswc17+emb_sg300_w": "BASE_SG_TL",
        "emb_extra_layer@iswc17+emb_kb200_rdf2vec_w": "BASE_KB_TL",
        "simple@iswc17": "BASE",
        "simple@iswc17+emb_kb200_rdf2vec_w+emb_sg300_w": "BASE_KB_SG",
        "emb_extra_layer@emb_kb200_rdf2vec_w+emb_sg300_w": "KB_SG_TL",
        "simple@emb_kb200_rdf2vec_w+emb_sg300_w": "KB_SG",
        "simple@iswc17+emb_sg300_w": "BASE_SG",
        "simple@iswc17+emb_kb200_rdf2vec_w": "BASE_KB"}


    def labels(methods):
        return [method_labels.get(method, method) for method in methods]


    def pcols(methods):
        return ["index_" + method for method in methods]


    def scols(methods):
        return ["score_" + method for method in methods]


    def best_predictions(expected, predicted, scores):
        p, r, s = precision_recall_curve(expected, predicted, scores)
        f1 = pr_fscore(p, r)
        threshold = s[np.argmax(f1)]
        predicted = predicted.copy()
        predicted[scores < threshold] = -1
        return predicted


    def pr_plot(entities, methods, joint, entity_type=None, prange=(0, 1), rrange=(0, 1), legendloc="lower right",
                title=None, ax=None, include=None):
        expected_col = "correct_joint"
        ents = entities
        if (not joint):
            ents = ents[ents.num_candidates > 1]
            expected_col = "correct"
        if (entity_type != None):
            ents = ents[ents.type == entity_type]
        if (not ax):
            ax = plt.subplot2grid((1, 1), (0, 0))
        if (include != None):
            df = pd.read_csv(include, sep="\t")
            twitter_df = df[df.method == "BASE"][["r", "p", "f1"]]
            dnn_df = df[df.method == "DNN"][["r", "p", "f1"]]
            ax.plot(twitter_df["r"], twitter_df["p"], "s-", label="TWITTER")
            ax.plot(dnn_df["r"], dnn_df["p"], "-", label="SIM_SAC2017")
        precision_recall_plot(ents, expected_col, pcols(methods), scols(methods), labels(methods),
                              prange=prange, rrange=rrange, legendloc=legendloc, title=title, ax=ax)


    def pr_table(entities, methods, joint):
        expected_col = "correct_joint"
        ents_all = entities
        if (not joint):
            ents_all = ents_all[ents_all.num_candidates > 1]
            expected_col = "correct"
        ents_per = ents_all[ents_all.type == "per"]
        ents_org = ents_all[ents_all.type == "org"]
        ref_method = methods[0]
        ref_pred_all = best_predictions(ents_all[expected_col], ents_all["index_" + ref_method],
                                        ents_all["score_" + ref_method])
        ref_pred_per = best_predictions(ents_per[expected_col], ents_per["index_" + ref_method],
                                        ents_per["score_" + ref_method])
        ref_pred_org = best_predictions(ents_org[expected_col], ents_org["index_" + ref_method],
                                        ents_org["score_" + ref_method])
        for method in methods:
            print("%s" % labels([method])[0].replace("_", "\\_"))

            def process(ents_filtered, ref_pred):
                expected = ents_filtered[expected_col]
                predicted = best_predictions(expected, ents_filtered["index_" + method],
                                             ents_filtered["score_" + method])
                mc = bootstrap_confidence_intervals(expected, predicted, pr_measures, iterations=10000, confidence=95)
                pvalues = pr_approximate_randomization(expected, ref_pred, predicted, iterations=10000)
                pss = "\\ss" if pvalues[0] < 0.05 else "   "
                rss = "\\ss" if pvalues[1] < 0.05 else "   "
                fss = "\\ss" if pvalues[2] < 0.05 else "   "
                print("& %.3f $\pm$%.3f%s & %.3f $\pm$%.3f%s & %.3f $\pm$%.3f%s" % (
                mc[0, 0], mc[1, 0], pss, mc[0, 1], mc[1, 1], rss, mc[0, 2], mc[1, 2], fss))

            process(ents_per, ref_pred_per)
            process(ents_org, ref_pred_org)
            process(ents_all, ref_pred_all)
            print("\\\\")


    def pairwise_table(candidates, methods):
        c_all = candidates[(candidates["index"] >= 0) & ((candidates["index"] < 3) | (candidates.correct == 1))]
        c_per = c_all[c_all.type == "per"]
        c_org = c_all[c_all.type == "org"]
        for method in methods:
            print("%s" % labels([method])[0])

            def process(cf):
                n = len(cf)
                expected = np.ones(n) * -1
                expected[np.asarray(cf.correct) == 1] = 1
                predicted = np.ones(n) * -1
                predicted[np.asarray(cf["score_" + method]) > 0.5] = 1
                mc = bootstrap_confidence_intervals(expected, predicted, pr_measures, iterations=100, confidence=95)
                pss = "   "
                rss = "   "
                fss = "   "
                print("& %.3f $\pm$%.3f%s & %.3f $\pm$%.3f%s & %.3f $\pm$%.3f%s" % (
                mc[0, 0], mc[1, 0], pss, mc[0, 1], mc[1, 1], rss, mc[0, 2], mc[1, 2], fss))

            process(c_per)
            process(c_org)
            process(c_all)


    entities = entity_table(candidates, 0.0)

    _plot_calibration = False
    _plot_acquisition = False
    _plot_overall = False
    _plot_selection = False
    _plot_internal = False

    _table_acquisition = False
    _table_overall = True
    _table_selection = False
    _table_pairwise = False  # broken, should use cross-entropy and convince readers  it is appropriate

    #    _method = "emb_extra_layer@iswc17+emb_kb200_rdf2vec_w+emb_sg300_w"
    #    _expected = entities["correct_joint"]
    #    _predicted = entities["index_" + _method]
    #    print(bootstrap_confidence_intervals(_expected, _predicted, pr_measures, iterations=10000, confidence=95))

    moverall = [
        "emb_extra_layer@iswc17_3_lsa_emb_fasttext_300_emb_glove_300+emb_kb200_rdf2vec_w+emb_sg300_w",
        "emb_extra_layer@iswc17_1_lsa+emb_kb200_rdf2vec_w+emb_sg300_w",
        "simple@iswc17_3_lsa_emb_fasttext_300_emb_glove_300"
    ]
    mselection = [
        "emb_extra_layer@iswc17_3_lsa_emb_fasttext_300_emb_glove_300+emb_kb200_rdf2vec_w+emb_sg300_w",
        "emb_extra_layer@iswc17_1_lsa+emb_kb200_rdf2vec_w+emb_sg300_w",
        "simple@iswc17_3_lsa_emb_fasttext_300_emb_glove_300"
    ]

    if (_table_acquisition):
        def compute(ents, label):
            n = len(ents)
            hsc = len(ents[ents.num_candidates > 1]) / n  # has some candidate
            htc = len(ents[ents.correct >= 0]) / n  # has true candidate
            ac = np.average(ents.num_candidates - 1)  # avg candidates
            tcai = np.average(ents[ents.correct >= 0].correct)  # true candidate avg. index
            print("%s & %.1f\\%% & %.1f\\%% & %.1f & %.1f \\\\" % (label, hsc * 100, htc * 100, ac, tcai))


        compute(entities[entities.type == "per"], "Persons")
        compute(entities[entities.type == "org"], "Organisations")
        compute(entities, "All entities")

    print("Table overall:")
    if (_table_overall):
        pr_table(entities, moverall, True)

    print("Table selection:")
    if (_table_selection):
        pr_table(entities, mselection, False)

    if (_table_pairwise):
        pairwise_table(candidates, [method for method in mselection if method != "ref"])

    if (_plot_calibration):
        plt.figure(figsize=(6, 10))
        plot_calibration(entities, moverall[0])
        plt.savefig(path.join(args.input, "eval_overall_calibration.pdf"))

    if (_plot_acquisition):
        plt.figure(figsize=(6, 3.5))
        plot_candidate_recall(entities)
        plt.savefig(path.join(args.input, "eval_acquisition_recall.pdf"))
        plt.figure(figsize=(6, 3.5))
        plot_candidate_histogram(entities)
        plt.savefig(path.join(args.input, "eval_acquisition_histogram.pdf"))

    if (_plot_overall):
        plt.figure(figsize=(8, 6.5))
        pr_plot(entities, moverall, True, prange=(.6, 1), legendloc="lower left")
        plt.savefig(path.join(args.input, "eval_overall_all.pdf"))
        plt.figure(figsize=(4, 3))
        pr_plot(entities, moverall, True, entity_type="per", prange=(.5, 1), legendloc=None)
        plt.savefig(path.join(args.input, "eval_overall_per.pdf"))
        plt.figure(figsize=(4, 3))
        pr_plot(entities, moverall, True, entity_type="org", prange=(.5, 1), legendloc=None)
        plt.savefig(path.join(args.input, "eval_overall_org.pdf"))

    if (_plot_selection):
        plt.figure(figsize=(6, 4.5))
        pr_plot(entities, mselection, False, prange=(.7, 1), rrange=(.7, 1), legendloc="lower left")
        plt.savefig(path.join(args.input, "eval_selection_all.pdf"))
        plt.figure(figsize=(4, 3))
        pr_plot(entities, mselection, False, entity_type="per", prange=(.5, 1), legendloc=None)
        plt.savefig(path.join(args.input, "eval_selection_per.pdf"))
        plt.figure(figsize=(4, 3))
        pr_plot(entities, mselection, False, entity_type="org", prange=(.5, 1), legendloc=None)
        plt.savefig(path.join(args.input, "eval_selection_org.pdf"))

    if (_plot_internal):
        #mall = list(method_labels.keys())
        mall = mselection
        # _min_improvements = np.linspace(0, 0.4, 2)
        _min_improvements = [0.0]
        _grid = (2 * len(_min_improvements), 6)
        _row = 0
        plt.figure(figsize=(30, 10 * len(_min_improvements)))
        for _min_imp in _min_improvements:
            ents = entity_table(candidates, float(_min_imp))
            pr_plot(ents, mall, False, entity_type="per", title="Selection PER - %.2f" % _min_imp,
                    ax=plt.subplot2grid(_grid, (_row, 0)), legendloc=None)
            pr_plot(ents, mall, False, entity_type="org", title="Selection ORG - %.2f" % _min_imp,
                    ax=plt.subplot2grid(_grid, (_row + 1, 0)), legendloc=None)
            pr_plot(ents, mall, False, title="Selection ALL - %.2f" % _min_imp,
                    ax=plt.subplot2grid(_grid, (_row, 1), rowspan=2, colspan=2))
            pr_plot(ents, mall, True, title="Joint ALL - %.2f" % _min_imp,
                    ax=plt.subplot2grid(_grid, (_row, 3), rowspan=2, colspan=2))
            pr_plot(ents, mall, True, entity_type="per", title="Joint PER - %.2f" % _min_imp,
                    ax=plt.subplot2grid(_grid, (_row, 5)), legendloc=None)
            pr_plot(ents, mall, True, entity_type="org", title="Joint ORG - %.2f" % _min_imp,
                    ax=plt.subplot2grid(_grid, (_row + 1, 5)), legendloc=None)
            _row += 2
        plt.savefig(path.join(args.input, _plot_file_all))

#    plot_approximate_randomization(entities, "simple@iswc17", False)