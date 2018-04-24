package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.utils.Deserializer;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds tweets that are a part of conversation, saves them and records all the infomation needed to extract the rest of the conversation
 */
public class ExtractConversationsFromTweets {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractConversationsFromTweets.class);

    private static final String TWEETS_PATH = "tweets-path";
    private static final String OUTPUT_PATH = "output-path";

    public static final String CONVERSATION_GRAPH = "conversation.graph.tsv.gz";
    public static final String DICTIONARY = "dictionary.tsv.gz";

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
        DataSet<Tuple3<Long, Long, JsonObject>> edges =
            getInput(env, input)
                .flatMap(new Deserializer())
                .filter(new LanguageFilter("en"))
                .flatMap(new ConversationExtractor())
                .distinct(0, 1);

        edges
            .project(0, 1)
            .output(new RobustTsvOutputFormat<>(new Path(output, CONVERSATION_GRAPH), true)).setParallelism(1);

        TypeHint<Tuple2<Long, String>> dictResult = new TypeHint<Tuple2<Long, String>>() {};
        edges
            .flatMap((FlatMapFunction<Tuple3<Long, Long, JsonObject>, Tuple2<Long, String>>) (value, out) -> {
                out.collect(new Tuple2<>(value.f0, "null"));
                out.collect(new Tuple2<>(value.f1, GSON.toJson(value.f2)));
            }).returns(dictResult)
            .groupBy(0)
            .reduce((value1, value2) -> value1.f1.equals("null") ? value2 : value1).returns(dictResult)
            .output(new RobustTsvOutputFormat<>(new Path(output, DICTIONARY), true)).setParallelism(1);

        env.execute();
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
        ExtractConversationsFromTweets extractor = new ExtractConversationsFromTweets();

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
