package eu.fbk.ict.fm.smt.services;

import twitter4j.*;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Our Twitter tooling on top of the regular Twitter instance
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TwitterService {
    private Twitter twitter;
    private Map<String, RateLimitStatus> limits = new HashMap<>();

    public static final String USERS_SEARCH = "users/search";

    @Inject
    public TwitterService(Twitter twitter) {
        this.twitter = twitter;
    }

    public Twitter getTwitter() {
        return twitter;
    }

    public ResponseList<User> searchUsers(String query) throws TwitterException {
        try {
            ResponseList<User> users = twitter.users().searchUsers(query, 0);
            limits.put(USERS_SEARCH, users.getRateLimitStatus());
            return users;
        } catch (TwitterException e) {
            limits.put(USERS_SEARCH, e.getRateLimitStatus());
            throw e;
        }
    }

    public RateLimitStatus getLimit(String limit) {
        return limits.get(limit);
    }
}
