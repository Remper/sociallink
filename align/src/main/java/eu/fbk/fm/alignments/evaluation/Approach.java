package eu.fbk.fm.alignments.evaluation;

import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;

/**
 * An abstraction on top of whatever can take a feature vector and output the prediction
 */
public interface Approach {
    double[] predict(FullyResolvedEntry entry);
}
