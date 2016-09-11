package eu.fbk.ict.fm.smt.api;

import eu.fbk.ict.fm.smt.services.TwitterService;
import eu.fbk.ict.fm.smt.util.InvalidAttributeResponse;
import eu.fbk.ict.fm.smt.util.Response;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;

/**
 * Returns user's profiles based on a query
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("profiles")
public class ProfilesController {
    @Inject
    TwitterService twitterService;

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
    public String getProfilesByNameWithTopic(@QueryParam("name") String name, @QueryParam("topic") String topic) {
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
        return getResult(name+" "+topic).respond();
    }

    private Response getResult(String query) {
        List<User> result = new LinkedList<>();
        Response response = Response.success(null);
        try {
            result.addAll(twitterService.searchUsers(query));
        } catch (TwitterException e) {
            e.printStackTrace();
            response.code = Response.GENERIC_ERROR;
            response.message = e.getErrorCode()+" "+e.getErrorMessage();
        }
        response.data = result;
        return response;
    }
}
