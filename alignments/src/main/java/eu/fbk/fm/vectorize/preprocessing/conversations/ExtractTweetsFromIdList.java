package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.NavigableSet;

import static org.mapdb.Serializer.LONG;

/**
 * Finds tweets using the list of IDs
 */
public class ExtractTweetsFromIdList {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractConversationsFromTweets.class);

    private static final String TWEETS_PATH = "tweets-path";
    private static final String WORK_PATH = "work-path";

    private DataSet<String> getInput(ExecutionEnvironment env, Path input, Configuration parameters) {
        parameters.setBoolean("recursive.file.enumeration", true);

        return new DataSource<>(
            env,
            new TextInputFormat(input),
            BasicTypeInfo.STRING_TYPE_INFO,
            Utils.getCallLocationName()
        ).withParameters(parameters);
    }

    private void tweets(Path input, String work) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Load the list of IDs
        final Configuration parameters = new Configuration();

        //In memory DB
        DB db = DBMaker
                .memoryDirectDB()
                .allocateStartSize(5L   * 1024*1024*1024)
                .allocateIncrement(512L * 1024*1024)
                .make();

        NavigableSet<Long> tweetIds = db.treeSet("tweet_ids", LONG).create();
        try (LineNumberReader reader = new LineNumberReader(new FileReader(new File(work, "dictionary.tsv")))) {
            String line;
            int counter = 0;
            while ((line = reader.readLine()) != null) {
                tweetIds.add(Long.valueOf(line));

                counter++;
                if (counter % 1000000 == 0) {
                    LOGGER.info(String.format("Loaded %.0fm ids", (double) counter / 1000000));
                }
            }
        }

        //Deserialize and convert
        getInput(env, input, parameters)
            .flatMap(new Deserializer())
            .filter(new TweetFilter(tweetIds))
            .distinct(0)
            .map(new Serializer())
            .output(new RobustTsvOutputFormat<>(new Path(work, "tweets.tsv"), true));

        env.execute();
    }

    public class Serializer extends RichMapFunction<Tuple2<Long, JsonObject>, Tuple2<Long, String>> implements JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        private transient Gson GSON = new Gson();

        @Override
        public void open(Configuration parameters) throws Exception {
            GSON = new Gson();
        }

        @Override
        public Tuple2<Long, String> map(Tuple2<Long, JsonObject> value) throws Exception {
            return new Tuple2<>(value.f0, GSON.toJson(value.f1));
        }
    }

    public class Deserializer extends RichFlatMapFunction<String, Tuple2<Long, JsonObject>> implements JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        private transient Gson GSON;

        @Override
        public void open(Configuration parameters) throws Exception {
            GSON = new Gson();
        }

        @Override
        public void flatMap(String status, Collector<Tuple2<Long, JsonObject>> out) {
            try {
                JsonObject object = GSON.fromJson(status, JsonObject.class);

                if (object == null) {
                    return;
                }

                addTweet(object, out);
            } catch (final Throwable e) {
                //Don't care much about thrown away records
            }
        }

        private void addTweet(JsonObject status, Collector<Tuple2<Long, JsonObject>> out) {
            final JsonObject retweet = get(status, JsonObject.class, "retweeted_status");
            if (retweet != null ) {
                addTweet(retweet, out);
            }

            final Long sourceId = get(status, Long.class, "id");
            if (sourceId == null) {
                return;
            }

            out.collect(new Tuple2<>(sourceId, status));
        }
    }


    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("w", WORK_PATH,
                        "work directory", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        ExtractTweetsFromIdList extractor = new ExtractTweetsFromIdList();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            final String outputPath = cmd.getOptionValue(WORK_PATH, String.class);

            //noinspection ConstantConditions
            extractor.tweets(new Path(tweetsPath), outputPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}

