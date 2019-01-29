package eu.fbk.fm.alignments.scorer;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.PrepareTrainingSet;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.fm.vectorize.preprocessing.text.TextExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;

import static eu.fbk.fm.alignments.scorer.TextScorer.Mode.*;
import static eu.fbk.fm.alignments.scorer.TextScorer.UserMode.*;

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

    public enum UserMode {
        PROFILE, USER_DATA
    }

    public static final BiFunction<User, DBpediaResource, String> DBPEDIA_TEXT_EXTRACTOR =
        (user, resource) -> resource
            .getDescriptions()
            .stream()
            .reduce("", (text1, text2) -> text1 + " " + text2);

    private SimilarityScorer scorer;

    private boolean includeStatus = true;
    private Mode mode = ALL;
    private UserMode userMode = PROFILE;

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

    public TextScorer profile() {
        userMode = PROFILE;
        return this;
    }

    public TextScorer userData() {
        userMode = USER_DATA;
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

    public static String getUserDataText(User user) {
        if (!(user instanceof UserData)) {
            return "";
        }
        TextExtractor extractor = new TextExtractor(true);
        JsonElement array = ((UserData) user)
                .get(PrepareTrainingSet.StatusesProvider.class)
                .orElse(new JsonArray());
        if (!array.isJsonArray()) {
            return "";
        }
        return StreamSupport
            .stream(((JsonArray) array).spliterator(), false)
            .map(extractor::map)
            .reduce("", (text1, text2) -> text1 + " " + text2);
    }

    @Override
    public double getFeature(User user, DBpediaResource resource) {
        List<String> resourceTexts = resource.getDescriptions();
        String userText;
        if (userMode == USER_DATA) {
            userText = getUserDataText(user);
        } else {
            userText = getUserText(user);
        }
        if (mode == ALL) {
            return this.scorer.score(userText, String.join(" ", resourceTexts));
        }

        double topScore = 0.0d;
        for (String text : resourceTexts) {
            double curScore = this.scorer.score(userText, text);
            if (curScore > topScore) {
                topScore = curScore;
            }
        }
        return topScore;
    }
}
