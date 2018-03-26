package eu.fbk.fm.profiling;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.BuildUserLSA;
import eu.fbk.fm.alignments.utils.flink.GzipTextOutputFormat;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

/**
 * Processes the stream and filters out everything that is unrelated to the list of users
 */
public class FilterUserData {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterUserData.class);

    private static final String RESULTS_PATH = "results-path";
    private static final String LIST_PATH = "list";
    private static final String TWEETS_PATH = "tweets-path";

    private String[] uids;

    public FilterUserData(String[] uids) {
        this.uids = uids;
    }

    public void startPipeline(String inputPath, String outputPath) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);

        final DataSet<String> text = new DataSource<>(
                env,
                new TextInputFormat(new Path(inputPath)),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(parameters);

        text
            .flatMap(new Deserializer(uids))
            .output(new TextOutputFormat<>(new Path(outputPath)));

        env.execute();
    }

    public static final class Deserializer implements FlatMapFunction<String, String>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;
        private static final Gson GSON = new Gson();

        private String[] uids;

        public Deserializer(String[] uids) {
            this.uids = uids;
        }

        public boolean checkIfShouldPassThrough(JsonObject object) {
            JsonObject entities = object.getAsJsonObject("entities");
            JsonObject userObject = object.getAsJsonObject("user");

            if (entities == null || userObject == null) {
                return false;
            }

            //Check if the author is in the list
            final String username = get(userObject, String.class, "screen_name");
            if (Arrays.stream(uids).anyMatch(username::equalsIgnoreCase)) {
                return true;
            }

            return false;

            /*//Check if one of the mentions is in the list
            JsonArray rawMentions = entities.getAsJsonArray("user_mentions");
            for (JsonElement rawMention : rawMentions) {
                String mentionName = get(rawMention.getAsJsonObject(), String.class, "screen_name");
                if (Arrays.stream(uids).anyMatch(mentionName::equalsIgnoreCase)) {
                    return true;
                }
            }

            //Check if the author of the original tweet is in the list
            final String originalAuthor = get(object, String.class, "retweeted_status", "user", "screen_name");
            if (originalAuthor != null && Arrays.stream(uids).anyMatch(originalAuthor::equalsIgnoreCase)) {
                return true;
            }

            //No match was found
            return false;*/
        }

        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                JsonObject object = GSON.fromJson(value, JsonObject.class);

                if (object == null) {
                    return;
                }

                if (checkIfShouldPassThrough(object)) {
                    out.collect(value);
                }
            } catch (final Throwable e) {
                //Don't care much about thrown away records
            }
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("l", LIST_PATH,
                        "specifies the file with the list of user handlers to filter", "FILE",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            //noinspection ConstantConditions
            final String resultsPath = cmd.getOptionValue(RESULTS_PATH, String.class);
            //noinspection ConstantConditions
            final String listPath = cmd.getOptionValue(LIST_PATH, String.class);

            String[] uids = Files.asCharSource(new File(listPath), Charsets.UTF_8)
                    .readLines().stream()
                    .map(line -> line.split(",")[1])
                    .toArray(String[]::new);
            LOGGER.info(String.format("Loaded %d uids", uids.length));

            FilterUserData extractor = new FilterUserData(uids);
            extractor.startPipeline(tweetsPath, resultsPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
