package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.embeddings.EmbeddingsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * ISWC17 + embeddings
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PAI18SimpleStrategy extends ISWC17Strategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(PAI18SimpleStrategy.class);
    private final List<FeatureVectorProvider> vectorProviders;

    public PAI18SimpleStrategy(DataSource source, String lsaPath) throws Exception {
        super(source, lsaPath);
        vectorProviders = new LinkedList<FeatureVectorProvider>(){{
            add(new EmbeddingsProvider(source, "kb300"));
            add(new EmbeddingsProvider(source, "sg300"));
        }};
    }

    @Override
    public double[] getScore(User user, DBpediaResource resource, int order) {
        Objects.requireNonNull(user);
        Objects.requireNonNull(resource);

        int totalFeatures = 0;
        LinkedList<double[]> features = new LinkedList<>();
        double[] iswcFeatures = super.getScore(user, resource, 0);
        features.add(iswcFeatures);
        totalFeatures += iswcFeatures.length;

        for (FeatureVectorProvider provider : vectorProviders) {
            double[] providedFeatures = provider.getFeatures(user, resource);
            if (providedFeatures.length == 0) {
                LOGGER.error(String.format(
                    "0 features detected for provider: %s, entity: %s, candidate: %s",
                    provider.getClass().getSimpleName(),
                    resource.getIdentifier(),
                    user.getScreenName()
                ));
            }
            features.add(providedFeatures);
            totalFeatures += providedFeatures.length;
        }

        double[] result = new double[totalFeatures];
        int pointer = 0;
        for (double[] subFeatures : features) {
            System.arraycopy(subFeatures, 0, result, pointer, subFeatures.length);
            pointer += subFeatures.length;
        }

        return result;
    }
}
