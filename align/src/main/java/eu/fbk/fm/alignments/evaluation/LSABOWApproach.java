package eu.fbk.fm.alignments.evaluation;

import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;

/**
 * LSA+BOW-based approach
 */
public class LSABOWApproach implements Approach {
    @Override
    public double[] predict(FullyResolvedEntry entry) {
        return new double[0];
    }
}
