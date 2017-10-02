package eu.fbk.ict.fm.smt.recode;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.utils.core.CommandLine;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Saves the list of suggestions for the specific user
 */
public class GetSuggestions {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetSuggestions.class);
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final String OUTPUT = "output";
    private static final String CONSUMER_KEY = "consumer-key";
    private static final String CONSUMER_SECRET = "consumer-secret";

    private String consumerKey;
    private String consumerSecret;

    public GetSuggestions(String consumerKey, String consumerSecret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
    }

    public void run(String output) throws IOException, TwitterException, InterruptedException {
        Credentials[] credentialsList;
        credentialsList = GSON.fromJson(new FileReader(new File(output, "credentials.json")), Credentials[].class);

        // Checking if all the credentials are filled
        Twitter authInstance = TwitterFactory.getSingleton();
        authInstance.setOAuthConsumer(consumerKey, consumerSecret);
        boolean needAuth = false;
        for (Credentials credentials : credentialsList) {
            if (credentials.token != null) {
                continue;
            }
            needAuth = true;
            credentials.consumerKey = consumerKey;
            credentials.consumerSecret = consumerSecret;

            if (credentials.pin == null) {
                RequestToken reqToken = authInstance.getOAuthRequestToken("oob", "read-write");
                credentials.reqToken = reqToken.getToken();
                credentials.reqTokenSecret = reqToken.getTokenSecret();
                credentials.reqUrl = reqToken.getAuthorizationURL();
                continue;
            }

            RequestToken reqToken = new RequestToken(credentials.reqToken, credentials.reqTokenSecret);
            AccessToken token = authInstance.getOAuthAccessToken(reqToken, credentials.pin);
            credentials.token = token.getToken();
            credentials.tokenSecret = token.getTokenSecret();
            credentials.reqUrl = null;
            credentials.reqToken = null;
            credentials.reqTokenSecret = null;
            credentials.pin = null;
        }

        if (needAuth) {
            Files.write(GSON.toJson(credentialsList), new File(output, "credentials.json"), Charsets.UTF_8);
            LOGGER.info("Need to perform authorization for some of the credentials before continuing. Halting.");
            return;
        }

        final GetSuggestions self = this;
        while (true) {
            synchronized (self) {
                LOGGER.info("Starting requesting suggestions");
            }
            Arrays.stream(credentialsList).parallel().forEach(credentials -> {
                try {
                    Twitter twitter = getConnection(credentials);

                    // Getting the list of categories
                    ResponseList<Category> categories = twitter.suggestedUsers().getSuggestedUserCategories();

                    // Creating directory for user
                    File currentDir = new File(output, credentials.username);
                    currentDir = new File(currentDir, new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()));
                    File catsFile = new File(currentDir, "cats.tsv");
                    Files.createParentDirs(catsFile);

                    // Saving them to file
                    try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(catsFile), CSVFormat.TDF)) {
                        for (Category category : categories) {
                            csvPrinter.printRecord(category.getName(), category.getSize(), category.getSlug());
                        }
                    }


                    for (Category category : categories) {
                        ResponseList<User> suggestions = null;
                        try {
                            suggestions = twitter.suggestedUsers().getUserSuggestions(category.getSlug());
                        } catch (TwitterException e) {
                            synchronized (self) {
                                LOGGER.info("["+credentials.username+"] Hit the rate limit. Sleeping. "+e.getErrorMessage());
                            }
                            Thread.sleep(1000 * 16 * 60);
                            suggestions = twitter.suggestedUsers().getUserSuggestions(category.getSlug());
                        }

                        String filename = "suggestions-" + category.getSlug() + ".tsv";
                        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(new File(currentDir, filename)), CSVFormat.TDF)) {
                            for (User user : suggestions) {
                                csvPrinter.printRecord(user.getId(), user.getScreenName(), user.getName());
                            }
                        }
                    }
                } catch (Exception e) {
                    synchronized (self) {
                        LOGGER.error("Something happened", e);
                    }
                }
            });

            try {
                Thread.sleep(1000 * 45 * 60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static Twitter getConnection(Credentials credentials) throws TwitterException {
        return new TwitterFactory(
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(credentials.consumerKey)
                        .setOAuthConsumerSecret(credentials.consumerSecret)
                        .setOAuthAccessToken(credentials.token)
                        .setOAuthAccessTokenSecret(credentials.tokenSecret).build()
        ).getInstance();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("o", OUTPUT,
                        "output directory where to store the results", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, CONSUMER_KEY,
                        "application consumer key", "KEY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, CONSUMER_SECRET,
                        "application consumer secret", "SECRET",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String output = cmd.getOptionValue(OUTPUT, String.class);
            final String consumerKey = cmd.getOptionValue(CONSUMER_KEY, String.class);
            final String consumerSecret = cmd.getOptionValue(CONSUMER_SECRET, String.class);

            GetSuggestions script = new GetSuggestions(consumerKey, consumerSecret);
            script.run(output);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }

    public static class Credentials extends TwitterCredentials {
        public String username;
        public String pin;
        public String reqToken;
        public String reqTokenSecret;
        public String reqUrl;
    }
}
