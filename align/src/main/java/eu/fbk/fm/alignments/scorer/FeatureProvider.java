package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

/**
 * Provides a single feature to the classifier
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface FeatureProvider extends FeatureVectorProvider {
    double getFeature(User user, DBpediaResource resource);

    default double[] getFeatures(User user, DBpediaResource resource){
        double[] result = new double[1];
        result[0] = getFeature(user, resource);
        return result;
    }
}
