package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

/**
 * Provides a single feature for the classifier
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface FeatureProvider {
    double getFeature(User user, DBpediaResource resource);
}
