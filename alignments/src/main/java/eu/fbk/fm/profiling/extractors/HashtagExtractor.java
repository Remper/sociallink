package eu.fbk.fm.profiling.extractors;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.profiling.extractors.LSA.BOW;
import eu.fbk.utils.math.SparseVector;
import eu.fbk.utils.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static eu.fbk.fm.profiling.extractors.Features.TempFeatureSet.Type.AVG;

public class HashtagExtractor implements Extractor<BOW, Vector>, JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagExtractor.class);

    protected final HashMap<String, Integer> dictionary;
    protected final HashMap<String, AtomicInteger> idf;
    protected final HashMap<String, Double> idfPrecomputed;
    protected boolean idfPrecomputedReady = false;
    protected final HashSet<String> authors;
    protected final ImmutableSet<String> uids;
    protected final AtomicInteger receivedTweets = new AtomicInteger();
    protected final AtomicInteger addedSamples = new AtomicInteger();

    public HashtagExtractor(ImmutableSet<String> uids) {
        this.dictionary = new HashMap<>();
        this.idf = new HashMap<>();
        this.idfPrecomputed = new HashMap<>();
        this.authors = new HashSet<>();
        this.uids = uids;
    }

    public void extract(JsonObject tweet, Features features) {
        extract(tweet, features, 0L);
    }

    private synchronized void registerInDict(String word) {
        if (dictionary.containsKey(word)) {
            return;
        }

        idf.put(word, new AtomicInteger());
        dictionary.put(word, dictionary.size());
    }

    private synchronized void registerAuthor(String author) {
        authors.add(author);
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

        final JsonObject originalTweet = get(tweet, JsonObject.class, "retweeted_status");
        if (originalTweet != null) {
            extract(originalTweet, features, timestamp);
        }

        if (!this.uids.contains(author)) {
            return;
        }

        //Check if one of the mentions is in the list
        final JsonArray rawHashtags = get(tweet, JsonArray.class, "entities", "hashtags");
        if (rawHashtags == null || rawHashtags.size() == 0) {
            return;
        }
        final BOW bow = new BOW();
        for (JsonElement rawHashtag : rawHashtags) {
            final String hashtagText = get(rawHashtag.getAsJsonObject(), String.class, "text").toLowerCase();
            registerInDict(hashtagText);
            idf.get(hashtagText).incrementAndGet();
            idfPrecomputedReady = false;
            bow.add(hashtagText);
        }
        registerAuthor(author);
        features.addFeatureSet(AVG, author, bow, timestamp, this);
        addedSamples.incrementAndGet();
    }

    public Features.TempFeatureSet<BOW> merge(Features.TempFeatureSet<BOW> f1, Features.TempFeatureSet<BOW> f2) {
        Features.TempFeatureSet<BOW> result = f1.clone();
        result.getFeatures().add(f2.getFeatures());

        return result;
    }

    public BOW cloneFeatures(BOW feature) {
        BOW newFeature = new BOW();
        newFeature.add(feature);
        return newFeature;
    }

    private double idf(String term) {
        return Math.log10(1 + (double) authors.size() / (1 + this.idf.get(term).get()));
    }

    private synchronized void precomputeIdf() {
        if (this.idfPrecomputedReady) {
            return;
        }
        if (this.idfPrecomputed.size() > 0) {
            throw new RuntimeException("IDF precomputation was invoked twice, make sure you don't extract features after they are baked");
        }
        LOGGER.info("  ["+getId()+"] IDF precomputation started");
        for (String term : this.idf.keySet()) {
            this.idfPrecomputed.put(term, this.idf(term));
        }
        LOGGER.info("  ["+getId()+"] IDF precomputation done");
        this.idfPrecomputedReady = true;
    }

    public Features.FeatureSet<Vector> fin(Features.TempFeatureSet<BOW> f1) {
        if (!this.idfPrecomputedReady) {
            precomputeIdf();
        }
        BOW features = f1.getFeatures();
        SparseVector result = new SparseVector();

        for (String term : features.termSet()) {
            float tfidf = (float) (features.tf(term) * idfPrecomputed.get(term));
            result.add(dictionary.get(term), tfidf);
        }

        return new Features.FeatureSet<>(
            f1.getName(),
            this.getId(),
            result,
            f1.getTimestamp()
        );
    }

    public String getId() {
        return "hashtag_extractor";
    }

    public String statsString() {
        return String.format("[%s] received: %d, added: %d, dict size: %d", getId(), receivedTweets.get(), addedSamples.get(), dictionary.size());
    }

    public Stream<DictTerm> getDictionary() {
        return dictionary.keySet().stream().map(term -> new DictTerm(term, dictionary.get(term), idfPrecomputed.get(term), idf.get(term).get()));
    }

    public static class DictTerm {
        public final String term;
        public final int index;
        public final double weight;
        public final int freq;

        public DictTerm(String term, int index, double weight, int freq) {
            this.term = term;
            this.index = index;
            this.weight = weight;
            this.freq = freq;
        }

        @Override
        public String toString() {
            return String.format("%d\t%s\t%f\t%d", index, term, weight, freq);
        }
    }
}
