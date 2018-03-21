package eu.fbk.fm.smt.recode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.fbk.utils.core.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static eu.fbk.fm.smt.recode.GetSuggestions.getConnection;

/**
 * Follow the listed people
 */
public class Follow {

    private static final Logger LOGGER = LoggerFactory.getLogger(Follow.class);
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final String OUTPUT = "output";

    public void run(String output) throws IOException, TwitterException, InterruptedException {
        GetSuggestions.Credentials[] credentialsList;
        credentialsList = GSON.fromJson(new FileReader(new File(output, "credentials.json")), GetSuggestions.Credentials[].class);

        for (GetSuggestions.Credentials credentials : credentialsList) {
            String prefix = credentials.username + "-";
            String[] toFollow = GSON.fromJson(new FileReader(new File(output, prefix+"to-follow-post.json")), String[].class);
            Twitter twitter = getConnection(credentials);

            for (String friend : toFollow) {
                try {
                    Object response = twitter.createFriendship(friend, true);
                    LOGGER.info("Friended "+friend+" with the following response: "+response.toString());
                } catch (TwitterException e) {
                    LOGGER.warn("Failed to friend "+friend+": "+e.toString());
                }
            }
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("o", OUTPUT,
                        "output directory where to store the results", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String output = cmd.getOptionValue(OUTPUT, String.class);

            Follow script = new Follow();
            script.run(output);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
