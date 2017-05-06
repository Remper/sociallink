package eu.fbk.fm.alignments.index;

import com.google.gson.*;
import eu.fbk.fm.alignments.index.db.tables.UserIndex;
import eu.fbk.fm.alignments.index.db.tables.UserObjects;
import eu.fbk.fm.alignments.index.db.tables.records.UserIndexRecord;
import eu.fbk.fm.alignments.index.db.tables.records.UserObjectsRecord;
import eu.fbk.fm.alignments.index.sink.AbstractPostgresSink;
import eu.fbk.fm.alignments.index.sink.PostgresFileSink;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * Creates user index from the large stream of Twitter data and store it into PostgreSQL
 */
public class BuildUserIndex implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BuildUserIndex.class);

    private static final String RESULTS_PATH = "results-path";
    private static final String TWEETS_PATH = "tweets-path";
    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";

    private OutputFormat<Tuple3<String, Long, Integer>> indexOutputFormat;
    private OutputFormat<Tuple2<Long, String>> objectsOutputFormat;

    private void start(Path input, Path output) throws Exception {
        final Configuration parameters = new Configuration();
        parameters.setString("db.file", output.getPath());

        indexOutputFormat = new PostgresFileSink<Tuple3<String, Long, Integer>>("index").testFile(parameters);
        objectsOutputFormat = new PostgresFileSink<Tuple2<Long, String>>("objects").testFile(parameters);

        startPipeline(input, parameters);
    }

    private void start(Path input, Configuration parameters) throws Exception {

        indexOutputFormat = new IndexPostgresSink();
        objectsOutputFormat = new UserObjectPostgresSink();

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


        DataSet<Tuple2<Long, JsonObject>> reducedUserObjects = tweets
                .flatMap(new UserObjectExtractor())
                .groupBy(0)
                .reduce(new LatestUserObjectReduce())
                .project(0, 1);

        reducedUserObjects
                .map(new Serializer())
                .output(objectsOutputFormat).withParameters(parameters);

        /*tweets
                .flatMap(new IndexExtractor())
                .groupBy(0, 1)
                .sum(2)
                .output(indexOutputFormat).withParameters(parameters);*/

        env.execute();
    }

    public static final class LatestUserObjectReduce implements
            GroupCombineFunction<Tuple3<Long, JsonObject, Integer>, Tuple3<Long, JsonObject, Integer>>,
            ReduceFunction<Tuple3<Long, JsonObject, Integer>>{

        @Override
        public void combine(Iterable<Tuple3<Long, JsonObject, Integer>> values, Collector<Tuple3<Long, JsonObject, Integer>> out) throws Exception {
            Tuple3<Long, JsonObject, Integer> maxValue = getMax(values);

            if (maxValue != null) {
                out.collect(maxValue);
            }
        }

        @Nullable
        private Tuple3<Long, JsonObject, Integer> getMax(Iterable<Tuple3<Long, JsonObject, Integer>> values) {
            Tuple3<Long, JsonObject, Integer> maxValue = null;

            for (Tuple3<Long, JsonObject, Integer> value : values) {
                if (maxValue == null || value.f2 > maxValue.f2) {
                    maxValue = value;
                }
            }

            return maxValue;
        }

        @Override
        public Tuple3<Long, JsonObject, Integer> reduce(Tuple3<Long, JsonObject, Integer> value1, Tuple3<Long, JsonObject, Integer> value2) throws Exception {
            return value1.f2 > value2.f2 ? value1 : value2;
        }
    }

    public static final class UserObjectPostgresSink extends AbstractPostgresSink<Tuple2<Long, String>, UserObjectsRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void writeRecord(Tuple2<Long, String> record) throws IOException {
            UserObjectsRecord dbRecord = context.newRecord(UserObjects.USER_OBJECTS);
            dbRecord.setUid(record.f0);
            dbRecord.setObject(record.f1);
            append(dbRecord);
        }
    }

    public static final class IndexPostgresSink extends AbstractPostgresSink<Tuple3<String, Long, Integer>, UserIndexRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void writeRecord(Tuple3<String, Long, Integer> record) throws IOException {
            UserIndexRecord indexRecord = context.newRecord(UserIndex.USER_INDEX);
            indexRecord.setFullname(record.f0);
            indexRecord.setUid(record.f1);
            indexRecord.setFreq(record.f2);
            append(indexRecord);
        }
    }

    public static final class UserObjectExtractor implements FlatMapFunction<JsonObject, Tuple3<Long, JsonObject, Integer>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(JsonObject status, Collector<Tuple3<Long, JsonObject, Integer>> out) {
            JsonObject userObject = status.getAsJsonObject("user");

            if (userObject == null) {
                return;
            }

            //ID
            Long id = get(userObject, Long.class, "id");
            if (id == null) {
                return;
            }

            Integer statuses = get(userObject, Integer.class, "statuses_count");
            if (statuses == null) {
                statuses = 0;
            }

            out.collect(new Tuple3<>(id, userObject, statuses));

            //Recursively process retweeted object
            JsonObject retweetedObject = status.getAsJsonObject("retweeted_status");
            if (retweetedObject != null) {
                this.flatMap(retweetedObject, out);
            }
        }
    }

    public static final class IndexExtractor implements FlatMapFunction<JsonObject, Tuple3<String, Long, Integer>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(JsonObject status, Collector<Tuple3<String, Long, Integer>> out) {
            JsonObject userObject = status.getAsJsonObject("user");

            if (userObject == null) {
                return;
            }

            //ID and name
            Long id = get(userObject, Long.class, "id");
            String name = get(userObject, String.class, "name");
            if (id == null || name == null) {
                return;
            }
            name = prepareString(name);
            if (name.length() > 0) {
                out.collect(new Tuple3<>(prepareString(name), id, 1));
            }

            //Mentions
            JsonArray mentions = get(status, JsonArray.class, "entities", "user_mentions");
            if (mentions != null) {
                for (JsonElement rawMention : mentions) {
                    String mentionName = get(rawMention, String.class, "name");
                    Long mentionId = get(rawMention, Long.class, "id");
                    if (mentionId == null || mentionName == null) {
                        LOGGER.error("Illegal mention name or mention ID");
                        continue;
                    }

                    mentionName = prepareString(mentionName);
                    if (mentionName.length() == 0) {
                        continue;
                    }

                    out.collect(new Tuple3<>(mentionName, mentionId, 1));
                }
            }

            //Recursively process retweeted object
            JsonObject retweetedObject = status.getAsJsonObject("retweeted_status");
            if (retweetedObject != null) {
                this.flatMap(retweetedObject, out);
            }
        }

        private static String prepareString(String source) {
            StringBuilder buffer = new StringBuilder();

            source.codePoints().forEach(new IntConsumer() {
                private boolean spaceAdded = true;

                @Override
                public void accept(int value) {
                    if (value == 0x00) {
                        return;
                    }

                    if (Character.isWhitespace(value)) {
                        spaceAdded = true;
                        return;
                    }

                    if (spaceAdded && buffer.length() > 0) {
                        buffer.append(' ');
                    }
                    spaceAdded = false;
                    buffer.appendCodePoint(value);
                }
            });

            return buffer.toString();
        }
    }

    public static final class Deserializer implements FlatMapFunction<String, JsonObject>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        private static final Gson GSON = new Gson();

        @Override
        public void flatMap(String value, Collector<JsonObject> out) {
            try {
                JsonObject object = GSON.fromJson(value, JsonObject.class);

                if (object == null) {
                    return;
                }

                final Long source = get(object, Long.class, "user", "id");
                if (source == null) {
                    return;
                }

                out.collect(object);
            } catch (final Throwable e) {
                //Don't care much about thrown away records
            }
        }
    }

    public static final class Serializer implements MapFunction<Tuple2<Long, JsonObject>, Tuple2<Long, String>> {

        private static final long serialVersionUID = 1L;

        private static final Gson GSON = new Gson();

        @Override
        public Tuple2<Long, String> map(Tuple2<Long, JsonObject> value) throws Exception {
            return new Tuple2<>(value.f0, GSON.toJson(value.f1));
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("c", DB_CONNECTION,
                        "connection string for the database", "DB",
                        CommandLine.Type.STRING, true, false, false)
                .withOption(null, DB_USER,
                        "user for the database", "USER",
                        CommandLine.Type.STRING, true, false, false)
                .withOption(null, DB_PASSWORD,
                        "password for the database", "PASSWORD",
                        CommandLine.Type.STRING, true, false, false)
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved (in this case the db params are not required)", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false);
    }

    public static void main(String[] args) throws Exception {
        BuildUserIndex extractor = new BuildUserIndex();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            // Extract tweets path
            if (!cmd.hasOption(TWEETS_PATH)) {
                throw new Exception("Insufficient configuration");
            }
            //noinspection ConstantConditions
            final Path tweetsPath = new Path(cmd.getOptionValue(TWEETS_PATH, String.class));

            if (cmd.hasOption(RESULTS_PATH)) {
                //noinspection ConstantConditions
                final Path results = new Path(cmd.getOptionValue(RESULTS_PATH, String.class));
                extractor.start(tweetsPath, results);
                return;
            }

            // Extract connection if necessary
            if (!cmd.hasOption(DB_CONNECTION)
                    || !cmd.hasOption(DB_USER)
                    || !cmd.hasOption(DB_PASSWORD)) {
                throw new Exception("Insufficient configuration");
            }

            final Configuration parameters = new Configuration();
            parameters.setString("db.connection", cmd.getOptionValue(DB_CONNECTION, String.class));
            parameters.setString("db.user", cmd.getOptionValue(DB_USER, String.class));
            parameters.setString("db.password", cmd.getOptionValue(DB_PASSWORD, String.class));

            extractor.start(tweetsPath, parameters);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
