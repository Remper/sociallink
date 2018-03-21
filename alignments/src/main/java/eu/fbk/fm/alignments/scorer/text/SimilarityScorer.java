package eu.fbk.fm.alignments.scorer.text;

/**
 * Provides score based on two input texts
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface SimilarityScorer {
    double score(String text1, String text2);
    DebuggableSimilarityScorer debug();

    interface DebuggableSimilarityScorer extends SimilarityScorer, Debuggable { }
}
