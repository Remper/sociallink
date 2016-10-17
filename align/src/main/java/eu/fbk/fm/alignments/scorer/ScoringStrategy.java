package eu.fbk.fm.alignments.scorer;

/**
 * Scoring strategy interface
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface ScoringStrategy {
    void fillScore(FullyResolvedEntry entry);
}
