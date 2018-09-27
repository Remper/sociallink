package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.embeddings.EntityDirectEmbeddings;
import eu.fbk.fm.alignments.scorer.embeddings.SocialGraphEmbeddings;
import eu.fbk.fm.alignments.scorer.text.MemoryEmbeddingsProvider;
import eu.fbk.fm.alignments.scorer.text.VectorProvider;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.util.*;

/**
 * ISWC17 + embeddings
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PAI18Strategy extends AbstractScoringStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(PAI18Strategy.class);
    protected final List<FeatureVectorProvider> vectorProviders;

    public PAI18Strategy(DataSource source) throws Exception {
        this(initProviders(source));
    }

    public PAI18Strategy(DataSource source, String lsaPath) throws Exception {
        this(initProviders(source, lsaPath));
    }

    public PAI18Strategy(DataSource source, String lsaPath, String embeddingsPath) throws Exception {
        this(initProviders(source, lsaPath, embeddingsPath));
    }

    private static LinkedList<FeatureVectorProvider> initProviders(DataSource source, String lsaPath) throws Exception {
        return new LinkedList<FeatureVectorProvider>(){{
            add(ISWC17Strategy.builder().source(source).lsaPath(lsaPath).build());
            add(new EntityDirectEmbeddings(source, "kb200_rdf2vec"));
            add(new SocialGraphEmbeddings(source, "sg300"));
        }};
    }

    private static LinkedList<FeatureVectorProvider> initProviders(DataSource source, String lsaPath, String embeddingsPath) throws Exception {
        VectorProvider provider = new MemoryEmbeddingsProvider(embeddingsPath, lsaPath);
        return new LinkedList<FeatureVectorProvider>(){{
            add(ISWC17Strategy.builder().source(source).lsaPath(lsaPath).build());
            add(ISWC17Strategy.builder().vectorProvider(provider).build());
            add(new EntityDirectEmbeddings(source, "kb200_rdf2vec"));
            add(new SocialGraphEmbeddings(source, "sg300"));
        }};
    }

    private static LinkedList<FeatureVectorProvider> initProviders(DataSource source) throws Exception {
        return new LinkedList<FeatureVectorProvider>(){{
            add(new EntityDirectEmbeddings(source, "kb200_rdf2vec"));
            add(new SocialGraphEmbeddings(source, "sg300"));
        }};
    }

    public PAI18Strategy(List<FeatureVectorProvider> vectorProviders) {
        this.vectorProviders = vectorProviders;
    }

    public void addProvider(FeatureVectorProvider provider) {
        this.vectorProviders.add(provider);
    }

    @Override
    public Map<String, double[]> getScore(User user, DBpediaResource resource, int order) {
        Objects.requireNonNull(user);
        Objects.requireNonNull(resource);

        HashMap<String, double[]> features = new HashMap<>();

        vectorProviders.parallelStream().map(provider -> {
            double[] providedFeatures = provider.getFeatures(user, resource);
            if (providedFeatures.length == 0) {
                LOGGER.error(String.format(
                        "0 features detected for provider: %s, entity: %s, candidate: %s",
                        provider.getClass().getSimpleName(),
                        resource.getIdentifier(),
                        user.getScreenName()
                ));
            }

            return new Tuple2<>(provider.getSubspaceId(), providedFeatures);
        }).forEach(tuple -> {
            synchronized (this) {
                features.put(tuple.f0, tuple.f1);
            }
        });

        return features;
    }
}
