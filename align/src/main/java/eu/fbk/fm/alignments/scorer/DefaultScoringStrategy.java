package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.text.BOWVectorProvider;
import eu.fbk.fm.alignments.scorer.text.CosineScorer;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.fm.alignments.utils.MLService;
import eu.fbk.fm.alignments.utils.ResourcesService;
import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.core.strings.JaroWinklerDistance;
import eu.fbk.utils.data.Configuration;
import eu.fbk.utils.data.DatasetRepository;
import eu.fbk.utils.data.dataset.CSVDataset;
import eu.fbk.utils.data.dataset.bow.FeatureMappingInterface;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import twitter4j.User;

import java.util.*;

/**
 * Default set of feature providers
 *
 * @author Yaroslav Nechaev (remper@me.com)
 * @deprecated
 */
public class DefaultScoringStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultScoringStrategy.class);

    private FeatureExtraction extraction = null;
    private FeatureMappingInterface mapping = null;
    private FeatureProvider[] providers = null;
    private HashMap<String, Statistics> statistics = new HashMap<>();
    private HashMap<String, Integer> typeTaxonomy = new HashMap<>();
    private HashMap<String, HomepageAlignment> homepageAlignments = new HashMap<>();
    private int numUniqueFeatures = 0;

    public DefaultScoringStrategy() { }

    public DefaultScoringStrategy(FeatureMappingInterface mapping) {
        this.mapping = mapping;
    }

    public static class HomepageAlignment {
        public String filteredId = null;
        public Set<String> ids = new HashSet<>();
    }

    public static class Statistics {
        public int type = 0;
        public int indegree = 0;
        public int outdegree = 0;
        public int pageLength = 0;
        public int pageViews = 0;
    }

    public synchronized void init() {
        if (providers != null) {
            return;
        }
        LOGGER.info("Initialising scoring strategy");
        try {
            ResourcesService provider = new ResourcesService();
            DatasetRepository repository = new DatasetRepository(new Configuration());
            CSVDataset statisticsDataset = provider.provideDbpediaStatistics(repository);
            CSVRecord record;
            while ((record = statisticsDataset.readNext()) != null) {
                Statistics stats = new Statistics();
                String type = record.get(1);
                if (!typeTaxonomy.containsKey(type)) {
                    typeTaxonomy.put(type, typeTaxonomy.size());
                }
                stats.type = typeTaxonomy.get(type);
                try {
                    stats.indegree = Integer.valueOf(record.get(2));
                    stats.outdegree = Integer.valueOf(record.get(3));
                    stats.pageLength = Integer.valueOf(record.get(4));
                    stats.pageViews = Integer.valueOf(record.get(5));
                } catch (NumberFormatException e) {
                    //ignore
                }
                statistics.put(record.get(0), stats);
            }
            statisticsDataset.close();
            LOGGER.info("Done DBpedia statistics");

            CSVDataset extractedAlignments = provider.provideExtractedHomepageAlignments(repository);
            while ((record = extractedAlignments.readNext()) != null) {
                HomepageAlignment alignment = homepageAlignments.get(record.get(0));
                if (alignment == null) {
                    alignment = new HomepageAlignment();
                    homepageAlignments.put(record.get(0), alignment);
                }
                alignment.ids.add(record.get(2).toLowerCase());
            }
            extractedAlignments.close();
            LOGGER.info("Done extracted alignments (" + homepageAlignments.size() + ")");

            CSVDataset filteredAlignments = provider.provideFilteredHomepageAlignments(repository);
            while ((record = filteredAlignments.readNext()) != null) {
                HomepageAlignment alignment = homepageAlignments.get(record.get(0));
                if (alignment == null) {
                    LOGGER.warn("Impossible thing happened with entity: " + record.get(0));
                    continue;
                }
                alignment.filteredId = record.get(2).toLowerCase();
            }
            filteredAlignments.close();
            LOGGER.info("Done filtered alignments (" + homepageAlignments.size() + ")");

            extraction = new MLService()
                    .turnOffStemmer()
                    .provideFeatureExtraction();
            if (mapping == null) {
                mapping = provider.provideNGrams(repository);
            }
            SimilarityScorer scorer = new CosineScorer(new BOWVectorProvider(extraction, mapping));
            providers = new FeatureProvider[]{
                    new VerifiedScorer(),
                    new NameScorer(),
                    new NameScorer(new JaroWinklerDistance()),
                    new NameScorer.ScreenNameScorer(new JaroWinklerDistance()),
                    //new DescriptionScorer(new MLProvider().provideFeatureExtraction()),
                    new TextScorer(scorer).all(),
                    new TextScorer(scorer).unified(),
                    new TextScorer(scorer).all().statusOff(),
                    new TextScorer(scorer).unified().statusOff(),
                    new FollowersFriendsRatioScorer(),
                    new FriendsScorer(),
                    new FollowersScorer(),
                    new ListedScorer(),
                    new StatusesScorer(),
                    new ActivityScorer()
            };
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Can't initialise scoring toolset");
        }
        numUniqueFeatures = providers.length + typeTaxonomy.size() + 5 + 3 - 1;
        LOGGER.info("Done. Num unique features: " + numUniqueFeatures);
    }

    public void fillScore(FullyResolvedEntry entry) {
        init();
        int order = 0;
        entry.features = new LinkedList<>();
        for (User user : entry.candidates) {
            if (user == null) {
                LOGGER.error("Candidate is null for entity: " + entry.entry.resourceId);
                continue;
            }
            double[] features = new double[numUniqueFeatures + (numUniqueFeatures * (numUniqueFeatures - 1)) / 2];
            Arrays.fill(features, 0.0d);
            int index = 0;
            for (FeatureProvider provider : providers) {
                features[index] = provider.getFeature(user, entry.resource);
                if (Double.isNaN(features[index])) {
                    features[index] = 0.0d;
                    LOGGER.warn("NaN detected for provider: " + provider.getClass().getSimpleName() + ", entity: " + entry.entry.resourceId + ", candidate: " + user.getScreenName());
                }
                index++;
            }

            //features[index] = new ReturnOrderScorer(order).getFeature(user, entry.resource);
            //index++;
            order++;

            Statistics stats = statistics.get(entry.resource.getIdentifier());
            if (stats != null) {
                features[index + stats.type] = 1.0d;
                index += typeTaxonomy.size();
                features[index] = stats.indegree;
                index++;
                features[index] = stats.outdegree;
                index++;
                features[index] = stats.pageLength;
                index++;
                features[index] = stats.pageViews;
                index++;
            }

            HomepageAlignment alignment = homepageAlignments.get(entry.resource.getIdentifier());
            if (alignment != null) {
                String username = user.getScreenName().toLowerCase();
                if (alignment.filteredId != null && alignment.filteredId.equals(username)) {
                    features[index] = 1.0d;
                }
                if (alignment.ids.contains(username)) {
                    features[index + 1] = alignment.ids.size() == 1 ? 1.0d : 0.0d;
                    features[index + 2] = 1.0d;
                }
            }
            index += 3;

            //Combinations of features
            for (int i = 0; i < numUniqueFeatures; i++) {
                for (int j = i + 1; j < numUniqueFeatures; j++) {
                    features[index] = features[i] * features[j];
                    index++;
                }
            }
            //entry.features.add(features);
        }
    }

    public double[] getScore(User user, DBpediaResource resource, int order) {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        init();
        StringBuilder sb = new StringBuilder();
        for (FeatureProvider provider : providers) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(provider.getClass().getSimpleName());
        }
        return this.getClass().getSimpleName() + " {" + sb.toString() + "}";
    }
}
