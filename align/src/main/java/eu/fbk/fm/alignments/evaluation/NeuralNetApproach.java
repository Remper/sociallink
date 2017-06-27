package eu.fbk.fm.alignments.evaluation;

import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;

/**
 * Neural Network-based approach (based on pokedem-models)
 */
public class NeuralNetApproach implements Approach {
    @Override
    public double[] predict(FullyResolvedEntry entry) {
        return new double[0];
    }
}
