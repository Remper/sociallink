package eu.fbk.fm.profiling.extractors;

import com.google.gson.JsonObject;
import eu.fbk.fm.profiling.extractors.LSA.BOW;
import eu.fbk.fm.profiling.extractors.LSA.LSM;

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.fbk.fm.profiling.extractors.Features.TempFeatureSet.Type.AVG;

public class MentionedTextExtractor extends TextExtractor {
    public MentionedTextExtractor(LSM lsa, List<String> uids) {
        super(lsa, uids);
    }

    public void extract(JsonObject tweet, Features features, Long inheritedTimestamp) {
        LinkedList<Features.TempFeatureSet> results = new LinkedList<>();
        String text = get(tweet, String.class, "text");
        String author = get(tweet, String.class, "user", "screen_name");
        Long timestamp = get(tweet, Long.class, "timestamp_ms");
        timestamp = timestamp == null ? inheritedTimestamp : timestamp;

        checkArgument(author != null, "Author can't be null");
        checkArgument(text != null, "Text can't be null");

        author = author.toLowerCase();

        if (this.uids.contains(author)) {
            BOW bow = new BOW(text);
            features.addFeatureSet(AVG, author, bow, timestamp, this);
        }

        final JsonObject originalTweet = get(tweet, JsonObject.class, "retweeted_status");
        if (originalTweet != null) {
            extract(originalTweet, features, timestamp);
        }
    }

    public String getId() {
        return "mentioned_text_extractor";
    }
}
