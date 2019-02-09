package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.ml.features.FeatureExtraction;
import twitter4j.User;

import java.util.List;
import java.util.Set;

/**
 * Score candidates based on a simple similarity of their descriptions
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class DescriptionScorer implements FeatureProvider {
    public static final String COMMENT_PROPERTY = "http://www.w3.org/2000/01/rdf-schema#comment";
    public static final String ABSTRACT_PROPERTY = "";

    protected FeatureExtraction extractor;

    public DescriptionScorer(FeatureExtraction extractor) {
        this.extractor = extractor;
    }

    @Override
    public double getFeature(User user, KBResource resource) {
        return getAverageDescriptionMatch(user, resource);
    }

    private double getAverageDescriptionMatch(User user, KBResource resource) {
        List<String> descriptions = resource.getDescriptions();

        if (descriptions.size() == 0) {
            return -1;
        }

        double score = 0;
        Set<String> descriptionTokens = extractor.extract(user.getDescription());
        int orgTokens = descriptionTokens.size();
        for (String title : descriptions) {
            Set<String> titleTokens = extractor.extract(title);
            titleTokens.retainAll(descriptionTokens);
            if (orgTokens != 0) {
                score += (float) titleTokens.size() / (float) orgTokens;
            }
        }
        score /= descriptions.size();

        return score;
    }
}