package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

/**
 * Provides a multiple features to the classifier
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface FeatureVectorProvider {
    double[] getFeatures(User user, DBpediaResource resource);
}
