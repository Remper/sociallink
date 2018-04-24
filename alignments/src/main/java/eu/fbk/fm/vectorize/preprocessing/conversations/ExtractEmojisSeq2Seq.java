package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.utils.Deserializer;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
        DataSet<Tuple2<String, String>> dataset =
            getInput(env, input)
                .flatMap(new Deserializer())
                .filter(new LanguageFilter("en"))
                .flatMap(new TextExtractor(true))
                .flatMap(new EmojiExtractor());

        dataset
            .output(new RobustTsvOutputFormat<>(new Path(output, EMOJI_DATASET), true)).setParallelism(1);

        env.execute();
    }

    private static class EmojiExtractor implements FlatMapFunction<String, Tuple2<String, String>>, JsonObjectProcessor {
        @Override
        public void flatMap(String text, Collector<Tuple2<String, String>> out) throws Exception {
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

            if (emoji.length() == 0) {
                return;
            }
            out.collect(new Tuple2<>(rawtext.toString(), emoji.toString()));
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
