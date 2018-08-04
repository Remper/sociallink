package eu.fbk.fm.smt.services;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.FakeEndpoint;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategyFactory;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.*;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.fm.alignments.twitter.TwitterService;
import eu.fbk.fm.smt.model.CandidatesBundle;
import eu.fbk.fm.smt.model.Score;
import eu.fbk.fm.smt.model.ScoreBundle;
import twitter4j.User;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.sql.DataSource;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Runs the entire alignment pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class OnlineAlignmentsService {
    @Inject
    private TwitterService twitter;

    @Inject
    private MLService mlService;

    @Inject
    private ModelEndpoint endpoint;

    @Inject
    private DataSource dataSource;

    @Inject
    @Named("lsaFilename")
    private String lsaFilename;

    private QueryAssemblyStrategy qaStrategy = null;

    private FakeEndpoint fakeResourceEndpoint;
    private FillFromIndex index;
    private ScoringStrategy scoringStrategy;

    private synchronized void init() {
        if (qaStrategy == null) {
            qaStrategy = QueryAssemblyStrategyFactory.def();
        }
        if (index == null) {
            fakeResourceEndpoint = new FakeEndpoint();
            index = new FillFromIndex(fakeResourceEndpoint, new AllNamesStrategy(), dataSource);
            try {
                scoringStrategy = new PAI18Strategy(dataSource, lsaFilename);
            } catch (Exception e) {
                throw new RuntimeException("Can't initialize OnlineAlignmnetsService", e);
            }
        }
    }

    public List<ScoreBundle> compare(DBpediaResource resource, List<User> candidates) {
        List<ScoreBundle> result = new LinkedList<>();
        SimilarityScorer[] scorers = null;
        try {
            scorers = mlService.getScorers();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (scorers == null || scorers.length == 0) {
            return result;
        }

        for (SimilarityScorer scorer : scorers) {
            result.add(new ScoreBundle(
                scorer.toString(),
                match(resource, candidates, scorer, true)
            ));
        }
        return result;
    }

    public CandidatesBundle.Resolved performCALive(DBpediaResource resource) {
        if (qaStrategy == null) {
            init();
        }

        List<User> users;
        try {
            users = twitter.searchUsers(qaStrategy.getQuery(resource));
        } catch (TwitterService.RateLimitException e) {
            e.printStackTrace();
            users = new LinkedList<>();
        }

        CandidatesBundle.Resolved result = CandidatesBundle.resolved("live");
        users.forEach(user -> result.addUser(new UserData(user)));
        return result;
    }

    public CandidatesBundle.Resolved performCASocialLink(DBpediaResource resource) {
        init();
        List<UserData> users = index.queryCandidates(resource);

        CandidatesBundle.Resolved result = CandidatesBundle.resolved("index");
        users.forEach(result::addUser);
        return result;
    }

    public ScoreBundle performCSSocialLink(DBpediaResource resource, List<User> candidates) {
        ScoreBundle result = new ScoreBundle("sociallink");
        candidates.forEach(user -> {
            double[] prediction = endpoint.predict(scoringStrategy.getScore(user, resource));
            result.scores.add(new Score(user.getScreenName(), prediction[1]));
        });
        result.prepare();

        return result;
    }

    public ScoreBundle compareWithDefault(DBpediaResource resource, List<User> candidates) {
        try {
            SimilarityScorer scorer = mlService.getDefaultScorer();
            return new ScoreBundle(scorer.toString(), match(resource, candidates, scorer, false));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
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
            scores.add(score);
        }
        scores.sort((o1, o2) -> Double.compare(o2.score, o1.score));
        return scores;
    }
}
