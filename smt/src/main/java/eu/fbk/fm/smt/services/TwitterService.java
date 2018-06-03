package eu.fbk.fm.smt.services;

import twitter4j.*;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Our Twitter tooling on top of the regular Twitter instance
 *
 * For CDI purposes it is ApplicationScoped to be the only point of authority when it comes to querying Twitter
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class TwitterService {
    private final List<TwitterInstance> twitter;

    public static final String USERS_SEARCH = "/users/search";
    public static final String USER_TIMELINE = "/statuses/user_timeline";
    public static final String FRIENDS_LIST = "/friends/list";
    public static final int MAX_VALUE = 100500;

    @Inject
    public TwitterService(Twitter[] twitter) {
        this.twitter = new LinkedList<>();
        for (Twitter connection : twitter) {
            this.twitter.add(new TwitterInstance(connection));
        }
    }

    public List<User> searchUsers(String query) throws RateLimitException {
        return getReadyInstance(USERS_SEARCH).searchUsers(query);
    }

    public List<User> getFriends(long uid) throws RateLimitException {
        return getReadyInstance(FRIENDS_LIST).getFriends(uid);
    }

    public List<Status> getStatuses(long uid) throws RateLimitException {
        return getReadyInstance(USER_TIMELINE).getStatuses(uid);
    }


    public TwitterInstance getReadyInstance(String method) throws RateLimitException {
        int minTime = MAX_VALUE;
        TwitterInstance minInstance = twitter.get(0);

        for (TwitterInstance instance : this.twitter) {
            long readyIn = instance.readyIn(method);
            if (readyIn == 0) {
                return instance;
            }

            if (minTime > readyIn) {
                minTime = (int) readyIn;
                minInstance = instance;
            }
        }

        if (minTime < 10) {
            try {
                Thread.sleep((minTime + 5) * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return minInstance;
        }

        throw new RateLimitException(minTime);
    }


    public static class TwitterInstance {
        private Map<String, RateLimitStatus> limits = new HashMap<>();
        public Twitter twitter;

        public TwitterInstance(Twitter twitter) {
            this.twitter = twitter;
        }

        public long readyIn(String method) {
            long curTime = new Date().getTime()/1000;

            RateLimitStatus status;
            try {
                status = getLimitStatus(method);
            } catch (TwitterException e) {
                return MAX_VALUE; //Never
            }
            if (status == null) {
                return 0; //No information — suggest trying
            }
            return status.getRemaining() > 0 ? 0 : Math.max(status.getResetTimeInSeconds() - curTime, 0);
        }

        private RateLimitStatus getLimitStatus(String method) throws TwitterException {
            RateLimitStatus status = limits.get(method);

            if (status != null) {
                return status;
            }

            limits.putAll(twitter.getRateLimitStatus());
            return limits.get(method);
        }

        private List<User> searchUsers(String query) {
            return processListResult(limits, USERS_SEARCH, () -> twitter.users().searchUsers(query, 0));
        }

        private List<Status> getStatuses(Long uid) {
            return processListResult(limits, USER_TIMELINE, () -> twitter.timelines().getUserTimeline(uid));
        }

        private List<User> getFriends(long uid) {
            return getFriends(uid, -1);
        }

        private List<User> getFriends(long uid, int cursor) {
            return processListResult(limits, FRIENDS_LIST, () -> twitter.friendsFollowers().getFriendsList(uid, cursor, 200));
        }

        private static <T> List<T> processListResult(
                Map<String, RateLimitStatus> limits,
                String method,
                TwitterSupplier<ResponseList<T>> supplier)
        {
            try {
                ResponseList<T> statuses = supplier.get();
                limits.put(method, statuses.getRateLimitStatus());
                return statuses;
            } catch (TwitterException e) {
                limits.put(method, e.getRateLimitStatus());
            }

            return new LinkedList<>();
        }

        private interface TwitterSupplier<T> {
            T get() throws TwitterException;
        }
    }

    public static class RateLimitException extends Exception {
        public RateLimitException(int remaining) {
            super("The API is out of capacity, please try again in "+remaining+" seconds");
        }
    }
}
