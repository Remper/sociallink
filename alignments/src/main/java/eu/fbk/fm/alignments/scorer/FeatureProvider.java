package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import twitter4j.User;

/**
 * Provides a single feature to the classifier
 *
 * @author Yaroslav Nechaev (remper@me.com)
 * @deprecated Will be deleted in 2.0
 */
public interface FeatureProvider extends FeatureVectorProvider {
    double getFeature(User user, KBResource resource);

    default double[] getFeatures(User user, KBResource resource){
        double[] result = new double[1];
        result[0] = getFeature(user, resource);
        return result;
    }
}
