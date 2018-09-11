package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.utils.Deserializer;
import eu.fbk.fm.alignments.utils.flink.GzipTextOutputFormat;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.fm.vectorize.preprocessing.conversations.LanguageFilter;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.api.common.operators.Order.DESCENDING;

/**
 * A Flink pipeline that extracts all text from tweets one sentence per line to be fed to embedding learning approach
 */
public class ExtractEmojiDistributionFromTweets {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractEmojiDistributionFromTweets.class);

    private static final String TWEETS_PATH = "tweets-path";
    private static final String OUTPUT_PATH = "output-path";
    private static final String LANGUAGE = "language";
    private static final Gson GSON = new Gson();

    private String language = null;

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
        DataSet<JsonObject> objects =
            getInput(env, input)
                .flatMap(new Deserializer());

        if (language != null) {
            objects = objects.filter(new LanguageFilter(language));
        }

        TypeHint emojiTuple = new TypeHint<Tuple2<Long, LinkedList<String>>>(){};
        TypeHint finalTuple = new TypeHint<Tuple2<Long, String>>(){};
        objects
            .flatMap(new ExtractEmoji())
            .groupBy(0)
            .reduce((ReduceFunction<Tuple2<Long, LinkedList<String>>>) (value1, value2) -> {
                LinkedList<String> result = new LinkedList<>();
                result.addAll(value1.f1);
                result.addAll(value2.f1);
                return new Tuple2<>(value1.f0, result);
            })
            .returns(emojiTuple)
            .map((MapFunction<Tuple2<Long, LinkedList<String>>, Tuple2<Long, String>>) value -> {
                HashMap<String, Integer> distribution = new HashMap<>();
                for (String emoji : value.f1) {
                    distribution.put(emoji, distribution.getOrDefault(emoji, 0)+1);
                }
                return new Tuple2<>(value.f0, GSON.toJson(distribution));
            })
            .returns(finalTuple)
            .output(new RobustTsvOutputFormat<>(new Path(output, "emoji_dist_"+language+".tsv.gz"), true)).setParallelism(1);

        env.execute();
    }

    public static final class ExtractEmoji implements FlatMapFunction<JsonObject, Tuple2<Long, LinkedList<String>>>, JsonObjectProcessor {

        @Override
        public void flatMap(JsonObject status, Collector<Tuple2<Long, LinkedList<String>>> out) throws Exception {
            // Getting user id
            Long userId = get(status, Long.class, "user", "id");

            // Getting tweet id
            Long id = get(status, Long.class, "id");

            // Get the original text
            String originalText = get(status, String.class, "text");
            if (originalText == null) {
                originalText = get(status, String.class, "full_text");
            }

            LinkedList<String> result = new LinkedList<>();

            originalText.codePoints().forEach(value -> {
                if ((value >= 0x1F600 && value <= 0x1F64F) // Emoticons
                    || (value >= 0x1F900 && value <= 0x1F9FF) // Supplemental Symbols and Pictograms
                    || (value >= 0x2600 && value <= 0x26FF) // Miscellaneous Symbols
                    || (value >= 0x2700 && value <= 0x27BF) // Dingbats
                    || (value >= 0x1F300 && value <= 0x1F5FF)) // Miscellaneous Symbols And Pictographs (Emoji)
                {
                    result.add(new String(new int[]{value}, 0, 1));
                }
            });

            if (result.size() > 0) {
                out.collect(new Tuple2<>(userId, result));
            }
        }
    }

    public static final class EmojiTuple extends Tuple2<Long, LinkedList<String>> {
        public EmojiTuple(Long value0, LinkedList<String> value1) {
            super(value0, value1);
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("o", OUTPUT_PATH,
                        "output directory", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("l", LANGUAGE,
                        "only include specified language", "LANGUAGE",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        ExtractEmojiDistributionFromTweets extractor = new ExtractEmojiDistributionFromTweets();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            final String outputPath = cmd.getOptionValue(OUTPUT_PATH, String.class);

            if (cmd.hasOption(LANGUAGE)) {
                extractor.setLanguage(cmd.getOptionValue(LANGUAGE, String.class));
            }

            //noinspection ConstantConditions
            extractor.tweets(new Path(tweetsPath), new Path(outputPath));
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }

    public void setLanguage(String language) {
        this.language = language;
    }
}
