package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.utils.Deserializer;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.fm.vectorize.preprocessing.ExpandTweet;
import eu.fbk.fm.vectorize.preprocessing.text.TextExtractor;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.IntConsumer;

public class ExtractEmojisSeq2Seq {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractConversationsFromTweets.class);

    private static final String TWEETS_PATH = "tweets-path";
    private static final String OUTPUT_PATH = "output-path";

    public static final String EMOJI_DATASET = "emoji_dataset.tsv.gz";
    public static final String EMOJI_USER_DICTIONARY = "emoji_user_dictionary.tsv.gz";
    public static final String[] COMPOSITE_EMOJIS = {":)", ";)"};

    private static final Gson GSON = new Gson();

    private DataSet<String> getInput(ExecutionEnvironment env, Path input) {
        return getInput(env, input, new Configuration());
    }

    private DataSet<String> getInput(ExecutionEnvironment env, Path input, Configuration parameters) {
        parameters.setBoolean("recursive.file.enumeration", true);

        return new DataSource<>(
                env,
                new TextInputFormat(input),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(parameters);
    }

    private void tweets(Path input, Path output) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Deserialize and convert
        DataSet<Tuple3<JsonObject, String, String>> dataset =
            getInput(env, input)
                .flatMap(new Deserializer())
                .flatMap(new ExpandTweet())
                .filter(new LanguageFilter("en"))
                .flatMap(new TweetProcessor());

        dataset
            .map(new ExpandUser())
            .output(new RobustTsvOutputFormat<>(new Path(output, EMOJI_DATASET), true)).setParallelism(1);

        dataset
            .map(new ExtractUser())
            .distinct(1)
            .output(new RobustTsvOutputFormat<>(new Path(output, EMOJI_USER_DICTIONARY), true)).setParallelism(1);


        env.execute();
    }

    private static class TweetProcessor implements FlatMapFunction<JsonObject, Tuple3<JsonObject, String, String>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;
        private static final TextExtractor textExtractor = new TextExtractor(true);
        private static final EmojiExtractor emojiExtractor = new EmojiExtractor();

        @Override
        public void flatMap(JsonObject value, Collector<Tuple3<JsonObject, String, String>> out) throws Exception {
            JsonObject user = get(value, JsonObject.class, "user");
            Tuple2<String, String> emoji = emojiExtractor.map(textExtractor.map(value));

            if (user != null && emoji.f0.length() > 0 && emoji.f1.length() > 0) {
                out.collect(new Tuple3<>(user, emoji.f0, emoji.f1));
            }
        }
    }

    private static class ExtractUser implements MapFunction<Tuple3<JsonObject, String, String>, Tuple3<String, Long, String>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;
        private static final Gson GSON = new Gson();

        @Override
        public Tuple3<String, Long, String> map(Tuple3<JsonObject, String, String> tweet) throws Exception {
            String author = get(tweet.f0, String.class, "screen_name");
            Long authorId = get(tweet.f0, Long.class, "id");
            if (author == null || authorId == null) {
                throw new Exception("Incorrect user object: "+tweet.f0.toString());
            }
            return new Tuple3<>(author, authorId, GSON.toJson(tweet.f0));
        }
    }

    private static class ExpandUser implements MapFunction<Tuple3<JsonObject, String, String>, Tuple4<String, Long, String, String>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple4<String, Long, String, String> map(Tuple3<JsonObject, String, String> tweet) throws Exception {
            String author = get(tweet.f0, String.class, "screen_name");
            Long authorId = get(tweet.f0, Long.class, "id");
            if (author == null || authorId == null) {
                throw new Exception("Incorrect user object: "+tweet.f0.toString());
            }
            return new Tuple4<>(author, authorId, tweet.f1, tweet.f2);
        }
    }

    private static class EmojiExtractor implements MapFunction<String, Tuple2<String, String>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, String> map(String text) throws Exception {
            StringBuilder rawtext = new StringBuilder();
            StringBuilder emoji = new StringBuilder();
            text.codePoints().forEach(value -> {
                if ((value >= 0x1F3FB && value <= 0x1F3FF) // Fitzpatrick diversity modifiers (skip)
                    || value == 0x200D) { // Glue character (skip)
                    return;
                }

                if ((value >= 0x1F600 && value <= 0x1F64F) // Emoticons
                    || (value >= 0x1F900 && value <= 0x1F9FF) // Supplemental Symbols and Pictograms
                    || (value >= 0x2600 && value <= 0x26FF) // Miscellaneous Symbols
                    || (value >= 0x2700 && value <= 0x27BF) // Dingbats
                    || (value >= 0x1F300 && value <= 0x1F5FF) // Miscellaneous Symbols And Pictographs (Emoji)
                    || (value >= 0x1F1E6 && value <= 0x1F1FF)) // Flags
                {
                    emoji.appendCodePoint(value);
                    return;
                }

                rawtext.appendCodePoint(value);
            });

            return new Tuple2<>(rawtext.toString(), emoji.toString());
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", OUTPUT_PATH,
                        "output directory", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        ExtractEmojisSeq2Seq extractor = new ExtractEmojisSeq2Seq();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            final String outputPath = cmd.getOptionValue(OUTPUT_PATH, String.class);

            //noinspection ConstantConditions
            extractor.tweets(new Path(tweetsPath), new Path(outputPath));
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
