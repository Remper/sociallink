package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

/**
 * Scoring strategy interface
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface ScoringStrategy {
    void fillScore(FullyResolvedEntry entry);

    default double[] getScore(User user, DBpediaResource resource) {
        return getScore(user, resource, 0);
    }

    double[] getScore(User user, DBpediaResource resource, int order);
}
