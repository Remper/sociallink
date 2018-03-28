package eu.fbk.fm.profiling.extractors;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.profiling.extractors.LSA.BOW;
import eu.fbk.fm.profiling.extractors.LSA.LSM;
import eu.fbk.utils.math.Vector;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.fbk.fm.profiling.extractors.Features.TempFeatureSet.Type.AVG;

public class TextExtractor implements Extractor<BOW, Vector>, JsonObjectProcessor {

    protected final LSM lsa;
    protected final ImmutableSet<String> uids;
    protected final AtomicInteger receivedTweets = new AtomicInteger();
    protected final AtomicInteger addedSamples = new AtomicInteger();

    public TextExtractor(LSM lsa, ImmutableSet<String> uids) {
        this.lsa = lsa;
        this.uids = uids;
    }

    public static class TextExtractorLSA extends TextExtractor {

        public TextExtractorLSA(LSM lsa, ImmutableSet<String> uids) {
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

    public void extract(JsonObject tweet, Features features) {
        extract(tweet, features, 0L);
    }

    public void extract(JsonObject tweet, Features features, Long inheritedTimestamp) {
        receivedTweets.incrementAndGet();
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
            addedSamples.incrementAndGet();
        }

        final JsonObject originalTweet = get(tweet, JsonObject.class, "retweeted_status");
        if (originalTweet != null) {
            extract(originalTweet, features, timestamp);
        }
    }

    public Features.TempFeatureSet<BOW> merge(Features.TempFeatureSet<BOW> f1, Features.TempFeatureSet<BOW> f2) {
        Features.TempFeatureSet<BOW> result = f1;
        synchronized (result) {
            result.getFeatures().add(f2.getFeatures());
        }

        return result;
    }

    public BOW cloneFeatures(BOW feature) {
        BOW newFeature = new BOW();
        newFeature.add(feature);
        return newFeature;
    }

    public Features.FeatureSet<Vector> fin(Features.TempFeatureSet<BOW> f1) {
        return new Features.FeatureSet<>(
            f1.getName(),
            this.getId(),
            lsa.mapDocument(f1.getFeatures()),
            f1.getTimestamp()
        );
    }

    public String getId() {
        return "text_extractor";
    }

    public String statsString() {
        return String.format("[%s] received: %d, added: %d", getId(), receivedTweets.get(), addedSamples.get());
    }
}
