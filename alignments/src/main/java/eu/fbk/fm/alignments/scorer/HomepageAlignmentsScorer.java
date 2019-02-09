package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.utils.ResourcesService;
import eu.fbk.utils.data.Configuration;
import eu.fbk.utils.data.DatasetRepository;
import eu.fbk.utils.data.dataset.CSVDataset;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import java.util.*;

/**
 * Provides a set of homepage-related features
 */
public abstract class HomepageAlignmentsScorer implements FeatureProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HomepageAlignmentsScorer.class);

    protected HashMap<String, HomepageAlignment> alignments;

    protected HomepageAlignmentsScorer(HashMap<String, HomepageAlignment> alignments) {
        this.alignments = alignments;
    }

    @Override
    public double getFeature(User user, KBResource resource) {
        HomepageAlignment alignment = alignments.get(resource.getIdentifier());

        return alignment == null ? 0.0d : match(user.getScreenName().toLowerCase(), alignment);
    }

    protected abstract double match(String username, HomepageAlignment alignment);

    public static List<FeatureProvider> createProviders() throws Exception {
        ResourcesService provider = new ResourcesService();
        DatasetRepository repository = new DatasetRepository(new Configuration());
        HashMap<String, HomepageAlignment> homepageAlignments = new HashMap<>();

        CSVRecord record;
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

        return new LinkedList<FeatureProvider>(){{
            add(new IDMatches(homepageAlignments));
            add(new IDExists(homepageAlignments));
            add(new IDUnique(homepageAlignments));
        }};
    }

    public static class HomepageAlignment {
        public String filteredId = null;
        public Set<String> ids = new HashSet<>();
    }

    public static final class IDMatches extends HomepageAlignmentsScorer {

        public IDMatches(HashMap<String, HomepageAlignment> alignments) {
            super(alignments);
        }

        @Override
        protected double match(String username, HomepageAlignment alignment) {
            return alignment.filteredId != null && alignment.filteredId.equals(username) ? 1.0d : 0.0d;
        }
    }

    public static final class IDExists extends HomepageAlignmentsScorer {

        public IDExists(HashMap<String, HomepageAlignment> alignments) {
            super(alignments);
        }

        @Override
        protected double match(String username, HomepageAlignment alignment) {
            return alignment.ids.contains(username) ? 1.0d : 0.0d;
        }
    }

    public static final class IDUnique extends HomepageAlignmentsScorer {

        public IDUnique(HashMap<String, HomepageAlignment> alignments) {
            super(alignments);
        }

        @Override
        protected double match(String username, HomepageAlignment alignment) {
            return alignment.ids.size() == 1 && alignment.ids.contains(username) ? 1.0d : 0.0d;
        }
    }
}
