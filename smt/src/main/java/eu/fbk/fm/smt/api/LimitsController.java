package eu.fbk.fm.smt.api;

import eu.fbk.fm.alignments.twitter.TwitterService;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/**
 * Returns various limits information related to the API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Path("limits")
public class LimitsController {
    @Inject
    TwitterService twitterService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getLimits() {
        return "";
    }
}
