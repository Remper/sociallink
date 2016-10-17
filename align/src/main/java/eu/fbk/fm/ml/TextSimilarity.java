package eu.fbk.fm.ml;

import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.core.Stopwatch;
import eu.fbk.utils.data.dataset.bow.FeatureMapping;
import eu.fbk.utils.data.dataset.bow.FeatureMappingInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Scorer that concentrates multiple different fields from the profile and entity
 * and calculates similarity
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TextSimilarity {
    private static final Logger logger = LoggerFactory.getLogger(TextSimilarity.class);

    private FeatureExtraction extractor;
    private FeatureMappingInterface mapping;
    private boolean includeStatus = true;

    public TextSimilarity(FeatureExtraction extractor, FeatureMappingInterface mapping) {
        this.extractor = extractor;
        this.mapping = mapping;
    }

    public TextSimilarity statusOff() {
        includeStatus = false;
        return this;
    }

    public double matchOnTexts(User user, String text) {
        return matchOnTexts(user, new LinkedList<String>() {{
            add(text);
        }});
    }

    public double matchOnTexts(User user, List<String> texts) {
        Stopwatch watch = Stopwatch.start();
        double maxMatch = 0.0d;

        for (String candidate : texts) {
            double score = getTextScore(user, candidate);
            if (score > maxMatch) {
                maxMatch = score;
            }
        }
        if (logger.isDebugEnabled() && maxMatch > 0.0d && texts.size() > 1) {
            logger.debug(String.format("Matching done in %.2f seconds, result: %.2f (%d texts)",
                    (double) watch.click() / 1000,
                    maxMatch,
                    texts.size()));
        }

        return maxMatch;
    }

    protected double getTextScore(User user, String text) {
        String userText = getUserText(user);

        double similarity = similarity(
                extractor.extract(userText),
                extractor.extract(text)
        );

        logger.debug("Similarity: " + similarity);
        return similarity;
    }

    protected double similarity(Set<String> text1, Set<String> text2) {
        //Compute similarity based on the remaining tokens
        double norm = norm(text1) * norm(text2);
        if (norm == 0.0d) {
            return 0.0d;
        }
        return dot(text1, text2) / norm;
    }

    protected double dot(Set<String> text1, Set<String> text2) {
        double sum = 0.0d;
        for (String token : text1) {
            if (!text2.contains(token)) {
                continue;
            }

            FeatureMapping.Feature feature = mapping.lookup(token);
            if (feature != null) {
                sum += Math.pow(feature.weight, 2.0d);
            }
        }
        return sum;
    }

    protected double norm(Set<String> text) {
        double sum = 0.0d;
        for (String token : text) {
            FeatureMapping.Feature feature = mapping.lookup(token);
            if (feature != null) {
                sum += Math.pow(feature.weight, 2.0d);
            }
        }
        return Math.sqrt(sum);
    }

    private String getUserText(User user) {
        StringBuilder userText = new StringBuilder();
        if (user.getDescription() != null) {
            userText.append(user.getDescription());
            userText.append(" ");
        }
        if (includeStatus && user.getStatus() != null) {
            userText.append(user.getStatus().getText());
            userText.append(" ");
        }
        if (user.getLocation() != null) {
            userText.append(user.getLocation());
        }
        return userText.toString().trim();
    }
}
