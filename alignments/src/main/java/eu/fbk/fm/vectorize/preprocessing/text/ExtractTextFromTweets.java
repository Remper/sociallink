package eu.fbk.fm.vectorize.preprocessing.text;

import eu.fbk.fm.alignments.index.utils.Deserializer;
import eu.fbk.fm.alignments.utils.flink.GzipTextOutputFormat;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.api.common.operators.Order.DESCENDING;

/**
 * A Flink pipeline that extracts all text from tweets one sentence per line to be fed to embedding learning approach
 */
public class ExtractTextFromTweets {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextFromTweets.class);

    private static final String TWEETS_PATH = "tweets-path";
    private static final String OUTPUT_PATH = "output-path";

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
        DataSet<String> tweets =
            getInput(env, input)
                .flatMap(new Deserializer())
                .flatMap(new TextExtractor(true))
                .filter((FilterFunction<String>) value -> value.split("\\s+").length > 3);

        //Count tokens
        TypeHint freqTuple = new TypeHint<Tuple2<String, Integer>>(){};
        DataSet<Tuple2<String, Integer>> freqDict =
            tweets.flatMap((value, out) -> {
                    for (String token : value.split("\\s+")) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                })
                .returns(freqTuple)
                .groupBy(0).sum(1)
                .filter((FilterFunction<Tuple2<String, Integer>>) value -> value.f1 > 5)
                .sortPartition(1, DESCENDING).setParallelism(1);

        freqDict
            .output(new RobustTsvOutputFormat<>(new Path(output, "dict.tsv.gz"), true)).setParallelism(1);

        tweets
            .output(new GzipTextOutputFormat<>(new Path(output, "tweet_text")));


        env.execute();
    }

    public static final class FreqTuple extends Tuple2<String, Integer> {
        public FreqTuple(String value0, Integer value1) {
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
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        ExtractTextFromTweets extractor = new ExtractTextFromTweets();

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
