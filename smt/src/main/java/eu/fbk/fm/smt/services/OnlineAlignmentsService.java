package eu.fbk.fm.smt.services;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.FakeEndpoint;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategyFactory;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import eu.fbk.fm.alignments.scorer.PAI18Strategy;
import eu.fbk.fm.alignments.scorer.ScoringStrategy;
import eu.fbk.fm.alignments.scorer.TextScorer;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
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

        ScoreBundle[] bundles = new ScoreBundle[scorers.length];
        int order = 0;
        for (SimilarityScorer scorer : scorers) {
            bundles[order] = new ScoreBundle(
                scorer.toString(),
                match(resource, candidates, scorer, true)
            );
            order++;
        }
        return bundles;
    }

    public FullyResolvedEntry performCASocialLink(DBpediaResource resource) {
        init();
        fakeResourceEndpoint.register(resource);
        FullyResolvedEntry entry = new FullyResolvedEntry(resource);
        index.fill(entry);
        fakeResourceEndpoint.release(resource.getIdentifier());

        return entry;
    }

    public ScoreBundle performCSSocialLink(FullyResolvedEntry entry) {
        ScoreBundle result = new ScoreBundle("sociallink");
        scoringStrategy.fillScore(entry);

        int[] i = new int[1];
        entry.features.forEach(features -> {
            double[] prediction = endpoint.predict(features);
            result.scores.add(new Score(entry.candidates.get(i[0]).getScreenName(), prediction[1]));
            i[0]++;
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
