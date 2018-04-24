package eu.fbk.fm.vectorize.preprocessing.conversations;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.RobustTsvOutputFormat;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static eu.fbk.fm.vectorize.preprocessing.conversations.ExtractConversationsFromTweets.CONVERSATION_GRAPH;
import static eu.fbk.fm.vectorize.preprocessing.conversations.ExtractConversationsFromTweets.DICTIONARY;

public class ExtractCompleteConversations {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractCompleteConversations.class);

    private static final String WORK_PATH = "work-path";

    private static final Gson GSON = new Gson();

    private DataSet<String> getInput(ExecutionEnvironment env, Path input) {
        return new DataSource<>(
                env,
                new TextInputFormat(input),
                BasicTypeInfo.STRING_TYPE_INFO,
                Utils.getCallLocationName()
        );
    }

    private void tweets(Path workDir) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Deserialize and convert
        TypeHint<Tuple2<JsonObject, Long>> firstJoin = new TypeHint<Tuple2<JsonObject, Long>>() {};
        TypeHint<Tuple2<JsonObject, JsonObject>> secondJoin = new TypeHint<Tuple2<JsonObject, JsonObject>>() {};
        TypeHint<Tuple2<String, String>> remap = new TypeHint<Tuple2<String, String>>() {};

        DataSet<Tuple2<Long, Long>> graph =
            getInput(env, new Path(workDir, CONVERSATION_GRAPH))
                .flatMap(new GraphDeserializer());

        DataSet<Tuple2<Long, JsonObject>> dictionary =
            getInput(env, new Path(workDir, DICTIONARY))
                .flatMap(new DictDeserializer());

        DataSet<Tuple2<JsonObject, JsonObject>> completeGraph = graph
            .joinWithHuge(dictionary).where(0).equalTo(0)
                .with((first, second) -> new Tuple2<>(second.f1, first.f1))
                .returns(firstJoin)
            .joinWithHuge(dictionary).where(1).equalTo(0)
                .with((first, second) -> new Tuple2<>(first.f0, second.f1))
                .returns(secondJoin);

        completeGraph
            .map(new SimpleTextExtractor())
            .output(new RobustTsvOutputFormat<>(new Path(workDir, "dataset"), true)).setParallelism(2);

        completeGraph
            .map(value -> new Tuple2<>(GSON.toJson(value.f0), GSON.toJson(value.f1)))
                .returns(remap)
            .output(new RobustTsvOutputFormat<>(new Path(workDir, "raw_dataset"), true)).setParallelism(2);

        env.execute();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("w", WORK_PATH,
                        "specifies the work directory which contains result of ExtractConversationsFromTweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        ExtractCompleteConversations extractor = new ExtractCompleteConversations();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String workPath = cmd.getOptionValue(WORK_PATH, String.class);

            //noinspection ConstantConditions
            extractor.tweets(new Path(workPath));
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }

    public static class SimpleTextExtractor implements MapFunction<Tuple2<JsonObject,JsonObject>, Tuple2<String, String>>, JsonObjectProcessor {

        public String extract(JsonObject object) {
            return get(object, String.class, "text")
                .replaceAll("\n", " ")
                .replaceAll("\t", " ")
                .replaceAll("\\s+", " ")
                .trim();
        }

        @Override
        public Tuple2<String, String> map(Tuple2<JsonObject, JsonObject> value) throws Exception {
            return new Tuple2<>(extract(value.f0), extract(value.f1));
        }
    }

    public static class DictDeserializer implements FlatMapFunction<String, Tuple2<Long, JsonObject>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        private static final Gson GSON = new Gson();

        @Override
        public void flatMap(String value, Collector<Tuple2<Long, JsonObject>> out) throws Exception {
            String[] row = value.split("\t");
            if (row.length > 2) {
                throw new Exception("Stop using split, it doesn't work");
            }
            if (row[1].equals("null")) {
                return;
            }

            JsonObject object = GSON.fromJson(row[1], JsonObject.class);

            out.collect(new Tuple2<>(Long.valueOf(row[0]), object));
        }
    }

    public static class GraphDeserializer implements FlatMapFunction<String, Tuple2<Long, Long>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<Long, Long>> out) throws Exception {
            String[] row = value.split("\t");

            out.collect(new Tuple2<>(Long.valueOf(row[0]), Long.valueOf(row[1])));
        }
    }

}
