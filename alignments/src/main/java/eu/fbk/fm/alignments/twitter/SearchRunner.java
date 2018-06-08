package eu.fbk.fm.alignments.twitter;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import org.apache.log4j.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.*;

/**
 * Runner that queries twitter until the batch is empty
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class SearchRunner implements Runnable {
    private static final Logger logger = Logger.getLogger(SearchRunner.class.getName());
    public static final int LIMIT = 180;

    private Twitter connection;
    private QueryAssemblyStrategy qaStrategy;
    private ResultReceiver receiver;
    private int processed = 0;

    //Side-effects
    private Thread curThread;
    private List<DBpediaResource> batch;
    private Date limitReset = null;
    private int limitLeft = 0;
    private String screenName = "";

    public SearchRunner(Twitter connection, QueryAssemblyStrategy qaStrategy, ResultReceiver receiver) throws TwitterException {
        this.connection = connection;
        this.screenName = connection.getScreenName();
        this.qaStrategy = qaStrategy;
        this.receiver = receiver;
    }

    public interface BatchProvider {
        List<DBpediaResource> provideNextBatch();

        void printStats(Date startDate, int processed);
    }

    public interface ResultReceiver {
        void processResult(List<User> candidates, DBpediaResource task);
    }

    @Override
    public void run() {
        int oldprocessed = processed;
        Date curDate = new Date();
        Random rnd = new Random();
        try {
            long rand = rnd.nextInt(20000);
            logger.info(String.format("Initial delay %.2f s", (float) rand / 1000));
            Thread.sleep(rand);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<User> users = new LinkedList<>();
        for (DBpediaResource resource : batch) {
            String query = qaStrategy.getQuery(resource);
            //Random delay to ensure that we don't destroy twitter
            try {
                Thread.sleep(rnd.nextInt(2000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (rnd.nextDouble() < 0.01) {
                logger.info(String.join(";", resource.getNames()) + " " + query);
            }
            try {
                users = connection.users().searchUsers(query, 0);
                setLimitReset(((ResponseList<User>) users).getRateLimitStatus());
                if (users.size() > 10) {
                    users.subList(10, users.size()).clear();
                }
            } catch (TwitterException e) {
                if (e.getErrorMessage().contains("Missing or invalid url parameter")) {
                    logger.info("Malformed URL for: " + query);
                } else if (e.exceededRateLimitation()) {
                    setLimitReset(e.getRateLimitStatus());
                    logger.info(String.format(
                            "Run complete, %d lookups done. Limit reached. Message: %s. Query: '%s'",
                            processed - oldprocessed,
                            e.getErrorMessage(),
                            query
                    ));
                    return;
                } else {
                    logger.error(e);
                }
            }
            receiver.processResult(users, resource);
            processed++;
        }
        long elapsed = new Date().getTime() - curDate.getTime();
        logger.info(String.format("Run complete, %d lookups done. %.2f seconds spent on run", processed - oldprocessed, (float) elapsed / 1000));
    }

    private void setLimitReset(RateLimitStatus status) {
        if (status == null) {
            return;
        }
        limitLeft = status.getRemaining();
        limitReset = new Date((status.getResetTimeInSeconds() + 2) * 1000L);
    }

    public void startWithBatch(List<DBpediaResource> batch) {
        if (!isReady()) {
            return;
        }

        this.batch = batch;
        this.curThread = new Thread(this);
        this.curThread.setName(this.screenName);
        this.curThread.start();
    }

    public Date readyAt() {
        if (limitReset == null || limitLeft > 0) {
            return new Date();
        }
        return limitReset;
    }

    public boolean isReady() {
        if (curThread == null) {
            return true;
        }
        if (!curThread.isAlive()) {
            curThread = null;
            batch = null;
            return true;
        }

        return false;
    }

    public int getProcessed() {
        return processed;
    }

    public static void startRun(SearchRunner[] runners, BatchProvider provider) {
        int oldprocessed = 0;
        Date startDate = new Date();
        while (true) {
            //Check alive status and amount of processed entities
            int processed = 0, alive = 0;
            for (SearchRunner runner : runners) {
                processed += runner.getProcessed();
                if (!runner.isReady()) {
                    alive++;
                }
            }
            logger.info(alive + " out of " + runners.length + " runners alive. " + (processed - oldprocessed) + " new entities processed");
            oldprocessed = processed;

            //Check runner status and warp in time according to limits
            Date latestReadyDate = new Date(new Date().getTime() + 5000);
            List<DBpediaResource> batch = null;
            if (alive == 0) {
                batch = provider.provideNextBatch();
                if (batch.size() == 0) {
                    logger.info("No more samples to process. Halting");
                    return;
                }
                for (SearchRunner runner : runners) {
                    Date runnerReadyDate = runner.readyAt();
                    if (runnerReadyDate.after(latestReadyDate)) {
                        latestReadyDate = runnerReadyDate;
                    }
                }
            }

            long waitTime = latestReadyDate.getTime() - new Date().getTime();
            if (waitTime > 0) {
                waitTime += 5000;
                if (waitTime > 300000) {
                    provider.printStats(startDate, processed);
                }
                logger.info(String.format("Sleeping for %.2f seconds", (float) waitTime / 1000));
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    return;
                }
            }

            //Start runners
            if (alive == 0) {
                Objects.requireNonNull(batch);
                provider.printStats(startDate, processed);
                int offset = 0;
                for (SearchRunner runner : runners) {
                    int limit = LIMIT;
                    if (offset >= batch.size()) {
                        break;
                    }
                    if (offset + limit > batch.size()) {
                        limit = batch.size() - offset;
                    }
                    runner.startWithBatch(batch.subList(offset, offset + limit));
                    offset += limit;
                }
                logger.info("Spawned " + runners.length + " runners");
            }
        }
    }

    public static SearchRunner[] generateRunners(
            TwitterCredentials[] credentials,
            QueryAssemblyStrategy qaStrategy,
            ResultReceiver receiver
    ) throws TwitterException {
        Twitter[] instances = createInstances(credentials);
        SearchRunner[] runners = new SearchRunner[credentials.length];
        for (int i = 0; i < credentials.length; i++) {
            runners[i] = new SearchRunner(instances[i], qaStrategy, receiver);
        }
        return runners;
    }

    public static Twitter[] createInstances(TwitterCredentials[] credentials) {
        Twitter[] instances = new Twitter[credentials.length];
        for (int i = 0; i < credentials.length; i++) {
            instances[i] = new TwitterFactory(
                new ConfigurationBuilder()
                    .setOAuthConsumerKey(credentials[i].consumerKey)
                    .setOAuthConsumerSecret(credentials[i].consumerSecret)
                    .setOAuthAccessToken(credentials[i].token)
                    .setOAuthAccessTokenSecret(credentials[i].tokenSecret)
                    .setTweetModeExtended(true)
                    .setJSONStoreEnabled(true)
                    .build()
            ).getInstance();
        }
        return instances;
    }
}
