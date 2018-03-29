package eu.fbk.fm.profiling.extractors;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.profiling.extractors.LSA.BOW;
import eu.fbk.fm.profiling.extractors.LSA.LSM;
import eu.fbk.utils.math.Vector;

import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.fbk.fm.profiling.extractors.Features.TempFeatureSet.Type.AVG;

public class MentionedTextExtractor extends TextExtractor {
    public MentionedTextExtractor(LSM lsa, ImmutableSet<String> uids) {
        super(lsa, uids);
    }

    public static class MentionedTextExtractorLSA extends MentionedTextExtractor {

        public MentionedTextExtractorLSA(LSM lsa, ImmutableSet<String> uids) {
            super(lsa, uids);
        }

        @Override
        public Features.FeatureSet<Vector> fin(Features.TempFeatureSet<BOW> f1) {
            return new Features.FeatureSet<>(
                f1.getName(),
                this.getId(),
                lsa.mapPseudoDocument(lsa.mapDocument(f1.getFeatures())),
                f1.getTimestamp()
            );
        }

        @Override
        public String getId() {
            return super.getId() + "_lsa";
        }
    }

    @Override
    public void extract(JsonObject tweet, Features features, Long inheritedTimestamp) {
        receivedTweets.incrementAndGet();
        LinkedList<Features.TempFeatureSet> results = new LinkedList<>();
        String text = get(tweet, String.class, "text");
        String author = get(tweet, String.class, "user", "screen_name");
        Long timestamp = get(tweet, Long.class, "timestamp_ms");
        timestamp = timestamp == null ? inheritedTimestamp : timestamp;

        checkArgument(text != null, "Text can't be null");

        //Check if one of the mentions is in the list
        final JsonArray rawMentions = get(tweet, JsonArray.class, "entities", "user_mentions");
        if (rawMentions == null) {
            return;
        }
        for (JsonElement rawMention : rawMentions) {
            final String mentionName = get(rawMention.getAsJsonObject(), String.class, "screen_name").toLowerCase();
            if (this.uids.contains(mentionName)) {
                final BOW bow = new BOW(text);
                addedSamples.incrementAndGet();
                features.addFeatureSet(AVG, mentionName, bow, timestamp, this);
            }
        }
    }

    @Override
    public String getId() {
        return "mentioned_text_extractor";
    }
}
