package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.embeddings.EmbeddingsProvider;
import eu.fbk.fm.alignments.scorer.embeddings.EntityEmbeddings;
import eu.fbk.fm.alignments.scorer.embeddings.SocialGraphEmbeddings;
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
    private final List<FeatureVectorProvider> vectorProviders;

    public PAI18Strategy(DataSource source, String lsaPath) throws Exception {
        vectorProviders = new LinkedList<FeatureVectorProvider>(){{
            add(new ISWC17Strategy(source, lsaPath));
            add(new EntityEmbeddings(source, "kb300"));
            add(new SocialGraphEmbeddings(source, "sg300"));
        }};
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
