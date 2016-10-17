package eu.fbk.ict.fm.smt.services;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.Evaluate;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategyFactory;
import eu.fbk.fm.alignments.scorer.DefaultScoringStrategy;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import eu.fbk.fm.alignments.scorer.ScoringStrategy;
import eu.fbk.utils.data.dataset.bow.FeatureMappingInterface;
import eu.fbk.utils.math.Scaler;
import org.jvnet.hk2.annotations.Service;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the entire alignment pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service @Singleton
public class OnlineAlignmentsService {
    @Inject
    private TwitterService twitter;

    @Inject
    private NGramsService ngrams;

    @Inject
    private Scaler scaler;

    @Inject
    private ModelEndpoint endpoint;

    private QueryAssemblyStrategy qaStrategy = null;
    private ScoringStrategy scoringStrategy = null;

    private void init() {
        qaStrategy = QueryAssemblyStrategyFactory.def();
        scoringStrategy = new DefaultScoringStrategy(ngrams);
    }

    public String getQuery(DBpediaResource resource) {
        return qaStrategy.getQuery(resource);
    }

    public synchronized Map<User, Double> produceAlignment(DBpediaResource resource) {
        if (qaStrategy == null) {
            init();
        }

        //Request twitter profiles
        HashMap<User, Double> result = new HashMap<>();
        List<User> users;
        try {
            users = twitter.searchUsers(qaStrategy.getQuery(resource));
        } catch (TwitterException e) {
            e.printStackTrace();
            return result;
        }

        //Calculate feature vectors
        FullyResolvedEntry entry = new FullyResolvedEntry(resource);
        entry.candidates = users;
        scoringStrategy.fillScore(entry);

        //Rescaling feature vector
        scaler.transform(entry.features);

        //Get predictions from the endpoint and write them to result
        int order = 0;
        for (User candidate : entry.candidates) {
            double[] prediction = endpoint.predict(entry.features.get(order));
            double score = -1.0d;
            if (prediction.length == 2) {
                score = prediction[1];
            }
            result.put(candidate, score);
        }

        return result;
    }
}
