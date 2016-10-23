package eu.fbk.ict.fm.smt.api;

import eu.fbk.fm.alignments.scorer.TextScorer;
import eu.fbk.fm.alignments.scorer.text.CosineScorer;
import eu.fbk.fm.alignments.scorer.text.SimilarityScorer;
import eu.fbk.ict.fm.smt.services.MLService;
import eu.fbk.ict.fm.smt.services.ResourcesService;
import eu.fbk.ict.fm.smt.services.TwitterService;
import eu.fbk.ict.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.ict.fm.smt.util.Response;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Returns user's profiles based on a query
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("profiles")
public class ProfilesController {
    @Inject
    TwitterService twitterService;

    @Inject
    MLService mlService;

    @Inject
    ResourcesService resources;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getProfilesByName(@QueryParam("name") String name) {
        if (name == null || name.length() <= 3) {
            return new InvalidAttributeResponse("name").respond();
        }
        return getResult(name).respond();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("topic")
    public String getProfilesByNameWithTopic(@QueryParam("name") String name, @QueryParam("topic") String topic) throws Exception {
        List<String> errors = new LinkedList<>();
        if (name == null || name.length() <= 3) {
            errors.add("name");
        }
        if (topic == null || topic.length() == 0) {
            errors.add("topic");
        }
        if (errors.size() != 0) {
            return new InvalidAttributeResponse(errors).respond();
        }

        List<User> result = new LinkedList<>();
        Response response = Response.success(result);
        try {
            result.addAll(twitterService.searchUsers(name));
        } catch (TwitterException e) {
            e.printStackTrace();
            response.code = Response.GENERIC_ERROR;
            response.message = e.getErrorCode() + " " + e.getErrorMessage();
            return response.respond();
        }
        if (result.size() == 0) {
            return response.respond();
        }

        TreeMap<Double, List<ScoredUser>> scoredUsers = new TreeMap<>();
        SimilarityScorer scorer = new CosineScorer(mlService.provideLSAVectors());
        TextScorer textScorer = new TextScorer(scorer);
        for (User curUser : result) {
            double score = scorer.score(textScorer.getUserText(curUser), topic);
            if (score > 0.0d) {
                List<ScoredUser> users = scoredUsers.getOrDefault(score, new LinkedList<>());
                users.add(new ScoredUser(curUser, score));
                scoredUsers.put(score, users);
            }
        }
        List<ScoredUser> processedResult = new LinkedList<>();
        for (Double score : scoredUsers.descendingKeySet()) {
            processedResult.addAll(scoredUsers.get(score));
        }
        response.data = processedResult;

        return response.respond();
    }

    private static class ScoredUser {
        User user;
        double score;

        public ScoredUser(User user, double score) {
            this.user = user;
            this.score = score;
        }
    }

    private Response getResult(String query) {
        List<User> result = new LinkedList<>();
        Response response = Response.success(null);
        try {
            result.addAll(twitterService.searchUsers(query));
        } catch (TwitterException e) {
            e.printStackTrace();
            response.code = Response.GENERIC_ERROR;
            response.message = e.getErrorCode() + " " + e.getErrorMessage();
        }
        response.data = result;
        return response;
    }
}
