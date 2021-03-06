package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

/**
 * Score candidates based on the API return order
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ReturnOrderScorer implements FeatureProvider {
    private int order;

    public ReturnOrderScorer(int order) {
        this.order = order;
    }

    @Override
    public double getFeature(User user, DBpediaResource resource) {
        return this.order;
    }
}
