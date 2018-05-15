package eu.fbk.fm.smt.services;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategyFactory;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import eu.fbk.fm.alignments.scorer.ScoringStrategy;
import eu.fbk.fm.alignments.scorer.TextScorer;
import eu.fbk.fm.alignments.scorer.text.Debuggable;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.fm.smt.model.Score;
import eu.fbk.fm.smt.model.ScoreBundle;
import eu.fbk.utils.math.Scaler;
import twitter4j.User;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Runs the entire alignment pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Singleton
public class OnlineAlignmentsService {
    @Inject
    private TwitterService twitter;

    @Inject
    private MLService mlService;

    @Inject
    private Scaler scaler;

    @Inject
    private ModelEndpoint endpoint;

    private QueryAssemblyStrategy qaStrategy = null;
    private ScoringStrategy scoringStrategy = null;

    private synchronized void init() {
        if (qaStrategy == null) {
            qaStrategy = QueryAssemblyStrategyFactory.def();
            //scoringStrategy = new DefaultScoringStrategy(mlService.getNgrams());
        }
    }

    public String getQuery(DBpediaResource resource) {
        return qaStrategy.getQuery(resource);
    }

    public synchronized List<User> populateCandidates(DBpediaResource resource) {
        if (qaStrategy == null) {
            init();
        }

        List<User> users;
        try {
            users = twitter.searchUsers(qaStrategy.getQuery(resource));
        } catch (TwitterService.RateLimitException e) {
            e.printStackTrace();
            return new LinkedList<>();
        }
        return users;
    }

    public ScoreBundle[] compare(DBpediaResource resource, List<User> candidates) {
        SimilarityScorer[] scorers = null;
        try {
            scorers = mlService.getScorers();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (scorers == null || scorers.length == 0) {
            return new ScoreBundle[0];
        }

        ScoreBundle[] bundles = new ScoreBundle[scorers.length+1];
        int order = 0;
        for (SimilarityScorer scorer : scorers) {
            bundles[order] = new ScoreBundle(
                scorer.toString(),
                match(resource, candidates, scorer, true)
            );
            order++;
        }
        bundles[order] = produceAlignment(resource, candidates);
        return bundles;
    }

    public List<Score> compareWithDefault(DBpediaResource resource, List<User> candidates) {
        try {
            return match(resource, candidates, mlService.getDefaultScorer(), false);
        } catch (IOException e) {
            e.printStackTrace();
            return new LinkedList<>();
        }
    }

    public List<Score> match(DBpediaResource resource, List<User> candidates, SimilarityScorer scorer, boolean debug) {
        List<Score> scores = new LinkedList<>();
        if (candidates.size() == 0) {
            return scores;
        }

        if (debug) {
            scorer = scorer.debug();
        }

        TextScorer textScorer = new TextScorer(scorer);
        for (User candidate : candidates) {
            Score score = new Score(candidate.getScreenName(), textScorer.getFeature(candidate, resource));
            if (debug) {
                score.debug = ((Debuggable) scorer).dump();
            }
            scores.add(score);
        }
        return scores;
    }

    public synchronized ScoreBundle produceAlignment(DBpediaResource resource, List<User> candidates) {
        ScoreBundle bundle = new ScoreBundle("alignments");
        if (candidates.size() == 0) {
            return bundle;
        }

        init();

        //Calculate feature vectors
        FullyResolvedEntry entry = new FullyResolvedEntry(resource);
        entry.candidates = candidates;
        scoringStrategy.fillScore(entry);

        //Rescaling feature vector
        //scaler.transform(entry.features);

        //Get predictions from the endpoint and write them to result
        int order = 0;
        for (User candidate : entry.candidates) {
            double[] prediction = endpoint.predict(entry.features.get(order));
            double score = -1.0d;
            if (prediction.length == 2) {
                score = prediction[1];
            }
            bundle.scores.add(new Score(candidate.getScreenName(), score));
        }

        return bundle;
    }
}
