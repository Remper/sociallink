package eu.fbk.fm.alignments.index;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.sink.PostgresFileSink;
import eu.fbk.fm.alignments.index.utils.Deserializer;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * Extract social graph from the stream and store it on disk
 */
public class ExtractSocialGraph {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractSocialGraph.class);

    private static final String RESULTS_PATH = "results-path";
    private static final String TWEETS_PATH = "tweets-path";

    private OutputFormat<Tuple3<Long, Long, Float>> forwardOutputFormat;
    private OutputFormat<Tuple3<Long, Long, Float>> backwardOutputFormat;

    private void start(Path input, Path output) throws Exception {
        final Configuration parameters = new Configuration();
        parameters.setString("db.file", output.getPath());

        forwardOutputFormat = new PostgresFileSink<Tuple3<Long, Long, Float>>("forward").testFile(parameters);
        backwardOutputFormat = new PostgresFileSink<Tuple3<Long, Long, Float>>("backward").testFile(parameters);

        startPipeline(input, parameters);
    }

    private void startPipeline(Path input, Configuration parameters) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        parameters.setBoolean("recursive.file.enumeration", true);

        final DataSet<String> text = new DataSource<>(
                env,
                new TextInputFormat(input),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        ).withParameters(parameters);

        //Deserialize and convert
        DataSet<JsonObject> tweets = text
                .flatMap(new Deserializer());

        processGraphAndOutput(tweets, new JointGraph(), parameters);

        env.execute();
    }

    private void processGraphAndOutput(DataSet<JsonObject> tweets, GraphEmitter dropper, Configuration parameters) {
        //Emit edges with frequences for the graph
        DataSet<Tuple3<Long, Long, Integer>> jointEdges = tweets
                .flatMap(dropper)
                .groupBy(0, 1)
                .sum(2)
                .filter(new GraphFilter(2));

        jointEdges
                .groupBy(0)
                .reduceGroup(new EdgeNormalizer())
                .output(forwardOutputFormat).withParameters(parameters);

        jointEdges
                .groupBy(1)
                .reduceGroup(new EdgeNormalizer())
                .output(backwardOutputFormat).withParameters(parameters);
    }

    private interface GraphEmitter extends FlatMapFunction<JsonObject, Tuple3<Long, Long, Integer>> {}

    private static final class JointGraph implements GraphEmitter, JsonObjectProcessor {

        @Override
        public void flatMap(JsonObject status, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            JsonObject userObject = status.getAsJsonObject("user");
            JsonObject entities = status.getAsJsonObject("entities");

            if (userObject == null || entities == null) {
                return;
            }

            //ID
            Long id = get(status, Long.class, "user", "id");
            if (id == null) {
                return;
            }

            //Original author
            Long originalAuthorId = get(status, Long.class, "retweeted_status", "user", "id");
            JsonObject retweetedObject = status.getAsJsonObject("retweeted_status");
            if (originalAuthorId != null) {
                out.collect(new Tuple3<>(id, originalAuthorId, 1));
                this.flatMap(retweetedObject, out);
            }

            //Mentions
            JsonArray rawMentions = entities.getAsJsonArray("user_mentions");
            for (JsonElement rawMention : rawMentions) {
                Long mentionId = get(rawMention, Long.class, "id");
                if (mentionId == null) {
                    continue;
                }
                out.collect(new Tuple3<>(id, mentionId, 1));
            }
        }
    }

    private static class EdgeNormalizer implements GroupReduceFunction<Tuple3<Long, Long, Integer>, Tuple3<Long, Long, Float>> {
        @Override
        public void reduce(Iterable<Tuple3<Long, Long, Integer>> edges, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            int sum = 0;

            LinkedList<Tuple3<Long, Long, Integer>> listedEdges = new LinkedList<>();
            for (Tuple3<Long, Long, Integer> edge : edges) {
                sum += edge.f2;
                listedEdges.add(edge);
            }

            for (Tuple3<Long, Long, Integer> edge : listedEdges) {
                out.collect(new Tuple3<>(
                        edge.f0,
                        edge.f1,
                        (float) edge.f2 / sum
                ));
            }
        }
    }

    private static class GraphFilter implements FilterFunction<Tuple3<Long, Long, Integer>> {
        private final int cutOffFrequency;

        public GraphFilter(int cutOffFrequency) {
            this.cutOffFrequency = cutOffFrequency;
        }

        @Override
        public boolean filter(Tuple3<Long, Long, Integer> value) throws Exception {
            return value.f2 >= cutOffFrequency;
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved (in this case the db params are not required)", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        ExtractSocialGraph extractor = new ExtractSocialGraph();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            // Extract tweets path
            if (!cmd.hasOption(TWEETS_PATH)) {
                throw new Exception("Insufficient configuration");
            }
            //noinspection ConstantConditions
            final Path tweetsPath = new Path(cmd.getOptionValue(TWEETS_PATH, String.class));

            //noinspection ConstantConditions
            final Path results = new Path(cmd.getOptionValue(RESULTS_PATH, String.class));
            extractor.start(tweetsPath, results);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
