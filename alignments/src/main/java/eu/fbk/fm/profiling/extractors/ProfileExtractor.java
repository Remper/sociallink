package eu.fbk.fm.profiling.extractors;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.profiling.extractors.LSA.BOW;
import eu.fbk.fm.profiling.extractors.LSA.LSM;
import eu.fbk.utils.core.ArrayUtils;
import eu.fbk.utils.math.DenseVector;
import eu.fbk.utils.math.Vector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.fbk.fm.profiling.extractors.Features.TempFeatureSet.Type.MAX;

public class ProfileExtractor implements Extractor<DenseVector, DenseVector>, JsonObjectProcessor {

    protected final LSM lsa;
    protected final ImmutableSet<String> uids;
    protected final AtomicInteger receivedTweets = new AtomicInteger();
    protected final AtomicInteger addedProfiles = new AtomicInteger();
    protected final HashMap<String, Integer> languages;

    public ProfileExtractor(LSM lsa, ImmutableSet<String> uids) {
        this.uids = uids;
        this.lsa = lsa;
        this.languages = new HashMap<>();
    }

    public void extract(JsonObject tweet, Features features) {
        extract(tweet, features, 0L);
    }

    public void extract(JsonObject tweet, Features features, Long inheritedTimestamp) {
        receivedTweets.incrementAndGet();
        LinkedList<Features.TempFeatureSet> results = new LinkedList<>();

        JsonObject user = get(tweet, JsonObject.class, "user");
        Long timestamp = get(tweet, Long.class, "timestamp_ms");
        timestamp = timestamp == null ? inheritedTimestamp : timestamp;

        checkArgument(user != null, "User object can't be null");

        String author = get(user, String.class, "screen_name");

        checkArgument(author != null, "Author can't be null");

        author = author.toLowerCase();

        if (this.uids.contains(author)) {
            float[] vector = new float[]{
                (float) registerLanguage(user),
                getIntFeature(user, "followers_count"),
                getIntFeature(user, "friends_count"),
                getIntFeature(user, "listed_count"),
                getIntFeature(user, "favourites_count"),
                getIntFeature(user, "statuses_count"),
                getBoolFeature(user, "protected"),
                getBoolFeature(user, "verified"),
                getBoolFeature(user, "geo_enabled"),
                getBoolFeature(user, "profile_background_tile"),
                getBoolFeature(user, "profile_use_background_image"),
                getBoolFeature(user, "default_profile"),
                getBoolFeature(user, "default_profile_image")
            };
            DenseVector textVector = getTextualFeature(user, "text");
            DenseVector result = new DenseVector(vector.length + textVector.size());
            for (int i = 0; i < vector.length; i++) {
                result.set(i, vector[i]);
            }
            for (int i = 0; i < textVector.size(); i++) {
                result.set(i+vector.length, textVector.get(i));
            }

            features.addFeatureSet(MAX, author, result, timestamp, this);
            addedProfiles.incrementAndGet();
        }

        final JsonObject originalTweet = get(tweet, JsonObject.class, "retweeted_status");
        if (originalTweet != null) {
            extract(originalTweet, features, timestamp);
        }
    }

    private int registerLanguage(JsonObject user) {
        String language = get(user, String.class, "lang");
        if (language == null) {
            language = "none";
        }
        if (languages.containsKey(language)) {
            return languages.get(language);
        }

        synchronized (this) {
            if (!languages.containsKey(language)) {
                languages.put(language, languages.size());
            }
        }
        return languages.get(language);
    }

    private DenseVector getTextualFeature(JsonObject user, String property) {
        String text = get(user, String.class, property);
        if (text == null) {
            text = "";
        }
        return (DenseVector) lsa.mapPseudoDocument(lsa.mapDocument(new BOW(text)));
    }

    private float getIntFeature(JsonObject user, String property) {
        Integer value = get(user, Integer.class, property);
        return value == null ? 0.f : (float) value;
    }

    private float getBoolFeature(JsonObject user, String property) {
        Boolean value = get(user, Boolean.class, property);
        return value == null || !value ? 0.0f : 1.0f;
    }

    @Override
    public Features.TempFeatureSet<DenseVector> merge(Features.TempFeatureSet<DenseVector> f1, Features.TempFeatureSet<DenseVector> f2) {
        if (f1.getTimestamp() >= f2.getTimestamp()) {
            return f1;
        }

        synchronized (f1) {
            if (f2.getTimestamp() > f1.getTimestamp()) {
                f1.setTimestamp(f2.getTimestamp());
                f1.setFeatures(f2.getFeatures());
            }
        }
        return f1;
    }

    @Override
    public DenseVector cloneFeatures(DenseVector feature) {
        throw new RuntimeException("Shouldn't be called");
    }

    @Override
    public Features.FeatureSet<DenseVector> fin(Features.TempFeatureSet<DenseVector> f1) {
        DenseVector f1vec = f1.getFeatures();
        DenseVector result = new DenseVector(languages.size()+f1vec.size()-1);
        result.set((int) f1.getFeatures().get(0), 1.0f);
        for (int i = 1; i < f1vec.size(); i++) {
            result.set(i + languages.size() - 1, f1vec.get(i));
        }

        return new Features.FeatureSet<>(
            f1.getName(),
            this.getId(),
            result,
            f1.getTimestamp()
        );
    }

    public Stream<String> getDictionary() {
        return languages.keySet().stream().map(lang -> String.format("%d\t%s", languages.get(lang), lang));
    }

    @Override
    public String getId() {
        return "profile_extractor";
    }

    public String statsString() {
        return String.format("[%s] received: %d, added: %d", getId(), receivedTweets.get(), addedProfiles.get());
    }
}
