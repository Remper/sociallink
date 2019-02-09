package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import twitter4j.User;

/**
 * Scorer that only depends on user object
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public abstract class AbstractUserScorer implements FeatureProvider {
    public abstract double score(User user);

    public double getFeature(User user, KBResource resource) {
        return score(user);
    }
}
