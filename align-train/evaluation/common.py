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


def draw_f1_lines(ax, rrange=(0, 1), prange=(0, 1)):
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
        ax.plot(r, p, "#d8d8d8")
        ax.text(rt, pt, "$F_1 = %.1f$" % f, color="#bbbbbb")