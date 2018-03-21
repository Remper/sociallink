package eu.fbk.fm.alignments.scorer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import java.util.LinkedList;

public abstract class AbstractScoringStrategy implements ScoringStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScoringStrategy.class);

    @Override
    public void fillScore(FullyResolvedEntry entry) {
        int order = 0;
        entry.features = new LinkedList<>();
        for (User user : entry.candidates) {
            if (user == null) {
                LOGGER.error("Candidate is null for entity: " + entry.entry.resourceId);
                continue;
            }

            entry.features.add(getScore(user, entry.resource, order));
            order++;
        }
    }
}
