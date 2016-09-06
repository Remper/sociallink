package eu.fbk.ict.fm.smt.api;

import com.google.gson.Gson;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Returns user's profiles based on a query
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("profiles")
public class ProfilesController {
    private static final Gson gson = new Gson();

    @Inject
    Twitter twitterConnection;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getProfilesByName(@QueryParam("name") String name) {
        if (name == null || name.length() <= 3) {
            return new InvalidAttributeResponse(new String[]{"name"}).respond();
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
            result.addAll(twitterConnection.users().searchUsers(query, 0));
        } catch (TwitterException e) {
            e.printStackTrace();
            response.code = Response.GENERIC_ERROR;
            response.message = e.getErrorCode()+" "+e.getErrorMessage();
        }
        response.data = result;
        return response;
    }

    private static class InvalidAttributeResponse extends Response {
        public String[] invalid;

        public InvalidAttributeResponse(Collection<String> invalid) {
            init();
            this.invalid = new String[invalid.size()];
            int i = 0;
            for (String invAttr : invalid) {
                this.invalid[i] = invAttr;
                i++;
            }
        }

        public InvalidAttributeResponse(String[] invalid) {
            init();
            this.invalid = invalid;
        }

        private void init() {
            this.code = INVALID_PARAMS_ERROR;
            this.message = INVALID_PARAMS_ERROR_MESSAGE;
            this.data = null;
        }
    }

    private static class Response {
        public static final int SUCCESS = 0;
        public static final String SUCCESS_MESSAGE = "ok";
        public static final int GENERIC_ERROR = -1;
        public static final int INVALID_PARAMS_ERROR = -2;
        public static final String INVALID_PARAMS_ERROR_MESSAGE = "invalid parameters";

        public int code;
        public String message;
        public Object data;

        public static Response success(@Nullable Object data) {
            Response response = new Response();
            response.data = data;
            response.code = SUCCESS;
            response.message = SUCCESS_MESSAGE;
            return response;
        }

        public String respond() {
            return gson.toJson(this);
        }
    }
}
