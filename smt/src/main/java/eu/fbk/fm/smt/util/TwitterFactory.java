package eu.fbk.fm.smt.util;

import twitter4j.Twitter;
import twitter4j.conf.ConfigurationBuilder;

import javax.inject.Inject;
import java.util.function.Supplier;

/**
 * Factory for Twitter object
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TwitterFactory implements Supplier<Twitter[]> {

    private final TwitterCredentials[] credentials;

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

    @Override
    public Twitter[] get() {
        Twitter[] instances = new Twitter[credentials.length];
        for (int i = 0; i < credentials.length; i++) {
            instances[i] = createInstance(credentials[i]);
        }
        return instances;
    }
}