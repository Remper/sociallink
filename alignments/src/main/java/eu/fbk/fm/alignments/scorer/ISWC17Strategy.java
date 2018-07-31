package eu.fbk.fm.alignments.scorer;

import com.google.common.base.Stopwatch;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.text.*;
import eu.fbk.utils.core.strings.JaroWinklerDistance;
import eu.fbk.utils.lsa.LSM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Strategy designed to work with our ISWC17 setup
 * A much lighter strategy that also utilises precomputed LSA for user text
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ISWC17Strategy extends AbstractScoringStrategy implements FeatureVectorProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(ISWC17Strategy.class);
    private final List<FeatureProvider> providers;

    private int numUniqueFeatures;
    private String id = "iswc17";

    public ISWC17Strategy(List<FeatureProvider> featureProviders) throws Exception {
        Stopwatch watch = Stopwatch.createStarted();
        providers = new LinkedList<FeatureProvider>(){{
            add(new VerifiedScorer());
            add(new NameScorer(new JaroWinklerDistance()));
            add(new NameScorer.ScreenNameScorer(new JaroWinklerDistance()));
            add(new FollowersFriendsRatioScorer());
            add(new FriendsScorer());
            add(new FollowersScorer());
            add(new ListedScorer());
            add(new StatusesScorer());
        }};
        providers.addAll(featureProviders);
        providers.addAll(HomepageAlignmentsScorer.createProviders());
        providers.addAll(EntityTypeScorer.createProviders());

        numUniqueFeatures = providers.size();
        LOGGER.info(String.format(
            "Done init. Num unique features: %d. Init done in: %.2f seconds",
            numUniqueFeatures,
            (float) watch.stop().elapsed(TimeUnit.MILLISECONDS) / 1000
        ));
    }

    @Override
    public Map<String, double[]> getScore(User user, DBpediaResource resource, int order) {
        return new HashMap<String, double[]>() {{
            put(getSubspaceId(), getFeatures(user, resource));
        }};
    }

    @Override
    public double[] getFeatures(User user, DBpediaResource resource) {
        Objects.requireNonNull(user);
        Objects.requireNonNull(resource);

        double[] features = new double[numUniqueFeatures + (numUniqueFeatures * (numUniqueFeatures - 1)) / 2];
        int index = 0;
        for (FeatureProvider provider : providers) {
            features[index] = provider.getFeature(user, resource);
            if (Double.isNaN(features[index])) {
                features[index] = 0.0d;
                LOGGER.warn(String.format(
                        "NaN detected for provider: %s, entity: %s, candidate: %s",
                        provider.getClass().getSimpleName(),
                        resource.getIdentifier(),
                        user.getScreenName()
                ));
            }
            index++;
        }

        //features[index] = new ReturnOrderScorer(order).getFeature(user, resource);
        //index++;

        //Combinations of features
        for (int i = 0; i < numUniqueFeatures; i++) {
            for (int j = i + 1; j < numUniqueFeatures; j++) {
                features[index] = features[i] * features[j];
                index++;
            }
        }
        return features;
    }

    @Override
    public String getSubspaceId() {
        return id;
    }

    public static ISWC17StrategyBuilder builder() {
        return new ISWC17StrategyBuilder();
    }

    public static class ISWC17StrategyBuilder {
        private DataSource source = null;
        private VectorProvider textVectorProvider = null;
        private List<VectorProvider> textVectorProviders = null;
        private String lsaPath = null;

        public ISWC17StrategyBuilder source(DataSource source) {
            this.source = source;
            return this;
        }

        public ISWC17StrategyBuilder vectorProvider(VectorProvider textVectorProvider) {
            this.textVectorProvider = textVectorProvider;
            return this;
        }

        public ISWC17StrategyBuilder vectorProviders(List<VectorProvider> textVectorProvider) {
            this.textVectorProviders = new LinkedList<>(textVectorProvider);
            return this;
        }

        public ISWC17StrategyBuilder lsaPath(String lsaPath) {
            this.lsaPath = lsaPath;
            return this;
        }

        private void initProvider(List<FeatureProvider> providers, VectorProvider textVectorProvider) {
            SimilarityScorer scorer = new CosineScorer(textVectorProvider);
            providers.add(new TextScorer(scorer).all().profile());
            if (source == null) {
                providers.add(new TextScorer(scorer).all().userData());
            } else {
                providers.add(new DBTextScorer(source, textVectorProvider));
            }
        }

        public ISWC17Strategy build() throws Exception {
            if (lsaPath != null && textVectorProvider == null) {
                LSM lsm = new LSM(lsaPath+"/X", 100, true);
                textVectorProvider = new LSAVectorProvider(lsm);
            }
            if (textVectorProvider == null && textVectorProviders == null) {
                throw new Exception("Requires vectorProvider(s) or LSA in order to be intitialised");
            }

            List<FeatureProvider> providers = new LinkedList<>();
            List<VectorProvider> vectorProviders = new LinkedList<>();
            if (textVectorProvider != null) {
                vectorProviders.add(textVectorProvider);
            }
            if (textVectorProviders != null) {
                vectorProviders.addAll(textVectorProviders);
            }
            for (VectorProvider vectorProvider : vectorProviders) {
                initProvider(providers, vectorProvider);
            }
            ISWC17Strategy strategy = new ISWC17Strategy(providers);
            strategy.id += "_" + vectorProviders.size() + "_" + vectorProviders
                .stream()
                .map(Object::toString)
                .reduce((suf1, suf2) -> suf1 + "_" + suf2)
                .orElse("none");
            return strategy;
        }
    }
}
