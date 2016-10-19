package eu.fbk.ict.fm.smt.services;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategyFactory;
import eu.fbk.fm.alignments.scorer.DefaultScoringStrategy;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import eu.fbk.fm.alignments.scorer.ScoringStrategy;
import eu.fbk.utils.math.Scaler;
import org.fbk.cit.hlt.core.lsa.BOW;
import org.fbk.cit.hlt.core.lsa.LSM;
import org.fbk.cit.hlt.core.math.Vector;
import org.jvnet.hk2.annotations.Service;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
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
    private MLService mlService;

    @Inject
    private Scaler scaler;

    @Inject
    private ModelEndpoint endpoint;

    @Inject
    @Named("lsaFilename")
    private String lsaFilename;
    private LSM lsa;

    private QueryAssemblyStrategy qaStrategy = null;
    private ScoringStrategy scoringStrategy = null;

    private synchronized void init() {
        if (qaStrategy == null) {
            qaStrategy = QueryAssemblyStrategyFactory.def();
            scoringStrategy = new DefaultScoringStrategy(ngrams);
        }
    }

    private synchronized void initLSA() throws IOException {
        if (lsa == null) {
            lsa = new LSM(lsaFilename, 100, true);
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
        } catch (TwitterException e) {
            e.printStackTrace();
            return new LinkedList<>();
        }
        return users;
    }

    private String getTexts(DBpediaResource resource) {
        List<String> texts = resource.getProperty(DBpediaResource.ABSTRACT_PROPERTY);
        texts.addAll(resource.getProperty(DBpediaResource.COMMENT_PROPERTY));
        return String.join(" ", texts);
    }

    private String getTexts(User user) {
        StringBuilder userText = new StringBuilder();
        if (user.getDescription() != null) {
            userText.append(user.getDescription());
            userText.append(" ");
        }
        if (user.getStatus() != null) {
            userText.append(user.getStatus().getText());
            userText.append(" ");
        }
        if (user.getLocation() != null) {
            userText.append(user.getLocation());
        }
        return userText.toString().trim();
    }

    public Map<String, Double> produceBasicSimilarity(DBpediaResource resource, List<User> candidates) {
        HashMap<String, Double> result = new HashMap<>();
        if (candidates.size() == 0) {
            return result;
        }

        for (User candidate : candidates) {
            double score = mlService.provideTextSimilarity().matchOnTexts(candidate, getTexts(resource));
            result.put(candidate.getScreenName(), score);
        }

        return result;
    }

    public LSASimilarity produceLSASimilarity(DBpediaResource resource, List<User> candidates) {
        LSASimilarity similarity = new LSASimilarity();
        similarity.lsa = new HashMap<>();
        similarity.vectorSim = new HashMap<>();
        if (candidates.size() == 0) {
            return similarity;
        }

        try {
            initLSA();
        } catch (IOException e) {
            e.printStackTrace();
            return similarity;
        }

        BOW resourceBow = new BOW(getTexts(resource));
        Vector resourceDocument = lsa.mapDocument(resourceBow);
        Vector resourcePseudo = lsa.mapPseudoDocument(resourceDocument);
        for (User candidate : candidates) {
            BOW candBow = new BOW(getTexts(candidate));
            Vector candDocument = lsa.mapDocument(candBow);
            Vector candPseudo = lsa.mapPseudoDocument(candDocument);

            double cosVSM = resourceDocument.dotProduct(candDocument)
                    / Math.sqrt(resourceDocument.dotProduct(resourceDocument) * candDocument.dotProduct(candDocument));
            double cosLSM = resourcePseudo.dotProduct(candPseudo)
                    / Math.sqrt(resourcePseudo.dotProduct(resourcePseudo) * candPseudo.dotProduct(candPseudo));
            similarity.lsa.put(candidate.getScreenName(), cosLSM);
            similarity.vectorSim.put(candidate.getScreenName(), cosVSM);
        }

        return similarity;
    }

    public static class LSASimilarity {
        public Map<String, Double> lsa;
        public Map<String, Double> vectorSim;
    }

    public synchronized Map<String, Double> produceAlignment(DBpediaResource resource, List<User> candidates) {
        HashMap<String, Double> result = new HashMap<>();
        if (candidates.size() == 0) {
            return result;
        }

        init();

        //Calculate feature vectors
        FullyResolvedEntry entry = new FullyResolvedEntry(resource);
        entry.candidates = candidates;
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
            result.put(candidate.getScreenName(), score);
        }

        return result;
    }
}
