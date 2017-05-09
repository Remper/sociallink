package eu.fbk.fm.alignments.scorer;

import com.google.common.base.Stopwatch;
import eu.fbk.fm.alignments.scorer.text.CosineScorer;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.fm.alignments.utils.ResourcesService;
import eu.fbk.utils.core.strings.JaroWinklerDistance;
import eu.fbk.utils.data.Configuration;
import eu.fbk.utils.data.DatasetRepository;
import eu.fbk.utils.data.dataset.CSVDataset;
import eu.fbk.utils.lsa.LSM;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Strategy designed to work with our ISWC17 setup
 * A much liter strategy that also utilises precomputed LSA for user text
 */
public class ISWC17Strategy implements ScoringStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ISWC17Strategy.class);
    private List<FeatureProvider> providers;

    private int numUniqueFeatures;

    public ISWC17Strategy(DataSource source, String lsaPath) throws Exception {
        Stopwatch watch = Stopwatch.createStarted();
        LSM lsm = new LSM(lsaPath+"/X", 100, true);
        SimilarityScorer scorer = new CosineScorer(new LSAVectorProvider(lsm));
        providers = new LinkedList<FeatureProvider>(){{
            add(new VerifiedScorer());
            add(new NameScorer());
            add(new NameScorer(new JaroWinklerDistance()));
            add(new NameScorer.ScreenNameScorer(new JaroWinklerDistance()));
            add(new TextScorer(scorer).all());
            add(new TextScorer(scorer).unified());
            add(new FollowersFriendsRatioScorer());
            add(new FriendsScorer());
            add(new FollowersScorer());
            add(new ListedScorer());
            add(new StatusesScorer());
            add(new ActivityScorer());
        }};
        providers.add(new DBTextScorer(source, new LSAVectorProvider(lsm)));
        providers.addAll(HomepageAlignmentsScorer.createProviders());
        providers.addAll(EntityTypeScorer.createProviders());

        numUniqueFeatures = providers.size() + 1;
        LOGGER.info(String.format(
                "Done. Num unique features: %d. Init done in: %.2f seconds",
                numUniqueFeatures,
                (float) watch.stop().elapsed(TimeUnit.MILLISECONDS) / 1000
        ));
    }

    @Override
    public void fillScore(FullyResolvedEntry entry) {
        int order = 0;
        entry.features = new LinkedList<>();
        for (User user : entry.candidates) {
            if (user == null) {
                LOGGER.error("Candidate is null for entity: " + entry.entry.resourceId);
                continue;
            }

            double[] features = new double[numUniqueFeatures + (numUniqueFeatures * (numUniqueFeatures - 1)) / 2];
            int index = 0;
            for (FeatureProvider provider : providers) {
                features[index] = provider.getFeature(user, entry.resource);
                if (Double.isNaN(features[index])) {
                    features[index] = 0.0d;
                    LOGGER.warn(String.format(
                            "NaN detected for provider: %s, entity: %s, candidate: %s",
                            provider.getClass().getSimpleName(),
                            entry.entry.resourceId,
                            user.getScreenName()
                    ));
                }
                index++;
            }

            features[index] = new ReturnOrderScorer(order).getFeature(user, entry.resource);
            index++;
            order++;

            //Combinations of features
            for (int i = 0; i < numUniqueFeatures; i++) {
                for (int j = i + 1; j < numUniqueFeatures; j++) {
                    features[index] = features[i] * features[j];
                    index++;
                }
            }
            entry.features.add(features);
        }
    }
}
