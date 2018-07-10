package eu.fbk.fm.alignments.twitter;

import eu.fbk.fm.alignments.Evaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.max;

/**
 * Our Twitter tooling on top of the regular Twitter instance
 *
 * For CDI purposes it is ApplicationScoped to be the only point of authority when it comes to querying Twitter
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TwitterService {

    private static final Logger logger = LoggerFactory.getLogger(Evaluate.class);
    private final List<TwitterInstance> twitter;

    public static final String USERS_SEARCH = "/users/search";
    public static final String USERS_SHOW = "/users/show";
    public static final String USER_TIMELINE = "/statuses/user_timeline";
    public static final String FRIENDS_LIST = "/friends/list";
    public static final int MAX_VALUE = 100500;


    public TwitterService(Twitter[] twitter) {
        this.twitter = new LinkedList<>();
        for (int i = 0; i < twitter.length; i++) {
            this.twitter.add(new TwitterInstance(i, twitter[i]));
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

    public User getProfile(long uid) throws RateLimitException {
        return getReadyInstance(USERS_SHOW).getProfile(uid);
    }

    public TwitterInstance getReadyInstance(String method) throws RateLimitException {
        int minTime = MAX_VALUE;
        TwitterInstance minInstance = twitter.get(0);

        List<TwitterInstance> ready = new ArrayList<>();
        for (TwitterInstance instance : this.twitter) {
            long readyIn = instance.readyIn(method);
            if (readyIn == 0) {
                ready.add(instance);
            }

            if (minTime > readyIn) {
                minTime = (int) readyIn;
                minInstance = instance;
            }
        }

        if (ready.size() > 0) {
            return ready.get(new Random().nextInt(ready.size()));
        }

        if (minTime < 10) {
            logger.info(String.format("Limit is soon (%ds), sleeping for a little bit", minTime));
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
        public final int id;
        public final AtomicInteger requests = new AtomicInteger(0);
        public final AtomicInteger autherrors = new AtomicInteger(0);

        public TwitterInstance(int id, Twitter twitter) {
            this.id = id;
            this.twitter = twitter;
        }

        public TwitterInstance(Twitter twitter) {
            this(0, twitter);
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
            return status.getRemaining() > 0 ? 0 : max(status.getResetTimeInSeconds() - curTime, 0);
        }

        private RateLimitStatus getLimitStatus(String method) throws TwitterException {
            if (!limits.containsKey(method)) {
                synchronized (this) {
                    if (!limits.containsKey(method)) {
                        limits.putAll(twitter.getRateLimitStatus());
                        if (!limits.containsKey(method)) {
                            limits.put(method, new Available()); //No information — suggest trying
                        }
                    }
                }
            }
            return limits.get(method);
        }

        private static class Available implements RateLimitStatus {

            @Override
            public int getRemaining() {
                return 1;
            }

            @Override
            public int getLimit() {
                return 180;
            }

            @Override
            public int getResetTimeInSeconds() {
                return 0;
            }

            @Override
            public int getSecondsUntilReset() {
                return 0;
            }
        }

        private static class NotAvailable implements RateLimitStatus {

            private long available;

            public NotAvailable(int seconds) {
                available = new Date().getTime() + 1000 * seconds;
            }

            @Override
            public int getRemaining() {
                return 0;
            }

            @Override
            public int getLimit() {
                return 180;
            }

            @Override
            public int getResetTimeInSeconds() {
                return (int) (available/1000);
            }

            @Override
            public int getSecondsUntilReset() {
                return max((int) ((available - new Date().getTime())/1000), 0);
            }
        }

        private synchronized List<User> searchUsers(String query) {
            return processListResult(limits, USERS_SEARCH, () -> twitter.users().searchUsers(query, 0));
        }

        private synchronized List<Status> getStatuses(Long uid) {
            return processListResult(limits, USER_TIMELINE, () -> twitter.timelines().getUserTimeline(uid));
        }

        private synchronized User getProfile(Long uid) {
            return processResult(limits, USERS_SHOW, () -> twitter.users().showUser(uid)).orElse(null);
        }

        private List<User> getFriends(long uid) {
            return getFriends(uid, -1);
        }

        private synchronized List<User> getFriends(long uid, int cursor) {
            return processListResult(limits, FRIENDS_LIST, () -> twitter.friendsFollowers().getFriendsList(uid, cursor, 200));
        }

        private <T> List<T> processListResult(
                Map<String, RateLimitStatus> limits,
                String method,
                TwitterSupplier<ResponseList<T>> supplier)
        {
            Optional<ResponseList<T>> result = processResult(limits, method, supplier);
            if (result.isPresent()) {
                return result.get();
            }
            return new LinkedList<>();
        }

        private <T extends TwitterResponse> Optional<T> processResult(
                Map<String, RateLimitStatus> limits,
                String method,
                TwitterSupplier<T> supplier)
        {
            int req = requests.incrementAndGet();
            if (req % 500 == 0) {
                logger.info(String.format("[Crawl instance %2d] Processed requests: %d, limit on latest method (%s): %d",
                    id, req, method,
                    Optional.ofNullable(limits.get(method)).map(RateLimitStatus::getRemaining).orElse(-1)
                ));
            }
            try {
                T statuses = supplier.get();
                limits.put(method, statuses.getRateLimitStatus());
                return Optional.of(statuses);
            } catch (TwitterException e) {
                processException(method, e);
            }

            return Optional.empty();
        }

        private void relax() {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void processException(String method, TwitterException e) {
            if (e.exceededRateLimitation()) {
                limits.put(method, e.getRateLimitStatus());
                return;
            }
            if (e.getMessage().contains("401:Authentication credentials")) {
                int err = autherrors.incrementAndGet();
                if (err % 100 == 0) {
                    logger.warn(String.format("[Crawl instance %2d] 401 errors: %d, total requests: %d", id, err, requests.get()));
                    relax();
                }
            } else if (!e.getMessage().contains("404:The URI requested is invalid") && e.getErrorCode() != 63) {
                logger.warn(String.format("[Crawl instance %2d] Unexpected rate limit exception: %s", id, e.getMessage()));
                return;
            }
            limits.put(method, e.getRateLimitStatus());
        }

        private interface TwitterSupplier<T> {
            T get() throws TwitterException;
        }
    }

    public static class RateLimitException extends Exception {
        public final int remaining;

        public RateLimitException(int remaining) {
            super("The API is out of capacity, please try again in "+remaining+" seconds");
            this.remaining = remaining;
        }
    }
}
