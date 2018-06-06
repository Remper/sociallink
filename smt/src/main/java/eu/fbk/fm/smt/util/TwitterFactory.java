package eu.fbk.fm.smt.util;

import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.fm.alignments.twitter.TwitterService;
import twitter4j.Twitter;
import twitter4j.conf.ConfigurationBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.function.Supplier;

/**
 * Factory for Twitter object
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class TwitterFactory implements Supplier<Twitter[]> {

    private final TwitterCredentials[] credentials;
    private Twitter[] instances = null;

    @Inject
    public TwitterFactory(TwitterCredentials[] credentials) {
        this.credentials = credentials;
    }

    private Twitter createInstance(TwitterCredentials credentials) {
        return new twitter4j.TwitterFactory(
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(credentials.consumerKey)
                        .setOAuthConsumerSecret(credentials.consumerSecret)
                        .setOAuthAccessToken(credentials.token)
                        .setOAuthAccessTokenSecret(credentials.tokenSecret).build()
        ).getInstance();
    }

    @Produces
    @Override
    public Twitter[] get() {
        synchronized (this) {
            if (instances == null) {
                instances = new Twitter[credentials.length];
                for (int i = 0; i < credentials.length; i++) {
                    instances[i] = createInstance(credentials[i]);
                }
            }
        }
        return instances;
    }

    @Produces
    public TwitterService produceTwitterService() {
        return new TwitterService(get());
    }
}