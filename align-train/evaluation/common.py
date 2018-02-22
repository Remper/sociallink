import warnings
import numpy as np


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

    # Discard NAN precision values and (precision, recall) pairs after recall reaches 1
    idx = np.logical_and(~np.isnan(p), ~np.isnan(r), np.diff(np.append(r, 2)) > 0)
    scores = scores[idx]
    p = p[idx]
    r = r[idx]

    # Return result
    return (p, r, scores)