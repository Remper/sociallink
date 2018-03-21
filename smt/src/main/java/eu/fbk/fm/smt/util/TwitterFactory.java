package eu.fbk.fm.smt.util;

import org.glassfish.hk2.api.Factory;
import twitter4j.Twitter;
import twitter4j.conf.ConfigurationBuilder;

import javax.inject.Inject;

/**
 * Factory for Twitter object
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class TwitterFactory implements Factory<Twitter[]> {

    private final TwitterCredentials[] credentials;

    @Inject
    public TwitterFactory(TwitterCredentials[] credentials) {
        this.credentials = credentials;
    }

    @Override
    public Twitter[] provide() {
        Twitter[] instances = new Twitter[credentials.length];
        for (int i = 0; i < credentials.length; i++) {
            instances[i] = createInstance(credentials[i]);
        }
        return instances;
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
    public void dispose(Twitter[] instance) {

    }
}