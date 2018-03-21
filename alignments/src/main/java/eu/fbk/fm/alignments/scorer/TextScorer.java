package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import java.util.List;

import static eu.fbk.fm.alignments.scorer.TextScorer.Mode.*;

/**
 * Scorer that concentrates multiple different fields from the profile and entity
 * and calculates similarity
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TextScorer implements FeatureProvider {
    private static final Logger logger = LoggerFactory.getLogger(TextScorer.class);

    public enum Mode {
        ALL, UNIFIED;
    }

    private SimilarityScorer scorer;

    private boolean includeStatus = true;
    private Mode mode = ALL;

    public TextScorer(SimilarityScorer scorer) {
        this.scorer = scorer;
    }

    public TextScorer statusOff() {
        includeStatus = false;
        return this;
    }

    public TextScorer all() {
        mode = ALL;
        return this;
    }

    public TextScorer unified() {
        mode = UNIFIED;
        return this;
    }

    public String getUserText(User user) {
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

    public static List<String> getResourceTexts(DBpediaResource resource) {
        List<String> texts = resource.getProperty(DBpediaResource.ABSTRACT_PROPERTY);
        texts.addAll(resource.getProperty(DBpediaResource.COMMENT_PROPERTY));

        return texts;
    }

    @Override
    public double getFeature(User user, DBpediaResource resource) {
        List<String> resourceTexts = getResourceTexts(resource);
        if (mode == ALL) {
            return this.scorer.score(getUserText(user), String.join(" ", resourceTexts));
        }

        double topScore = 0.0d;
        for (String text : resourceTexts) {
            double curScore = this.scorer.score(getUserText(user), text);
            if (curScore > topScore) {
                topScore = curScore;
            }
        }
        return topScore;
    }
}
