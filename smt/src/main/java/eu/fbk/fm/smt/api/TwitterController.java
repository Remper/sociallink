package eu.fbk.fm.smt.api;

import eu.fbk.fm.alignments.twitter.TwitterService;
import eu.fbk.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.fm.smt.util.Response;
import twitter4j.User;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;

/**
 * A special pipeline for the Recode Workshop
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("recode")
public class TwitterController {
    @Inject
    private TwitterService twitter;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("friends")
    public String findFriends(@QueryParam("uid") long uid) throws TwitterService.RateLimitException {
        List<String> errors = new LinkedList<>();
        if (uid <= 0) {
            errors.add("uid");
        }
        if (errors.size() > 0) {
            return new InvalidAttributeResponse(errors).respond();
        }

        List<User> friends = twitter.getFriends(uid);

        return Response.success(friends).respond();
    }
}
