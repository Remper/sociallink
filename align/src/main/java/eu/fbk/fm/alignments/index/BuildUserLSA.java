package eu.fbk.fm.alignments.index;

import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.index.sink.PostgresFileSink;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.alignments.utils.flink.TextInputFormat;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.DenseVector;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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

import javax.annotation.Nullable;

/**
 * Creates LSA representation of each user
 */
public class BuildUserLSA {

    private static final Logger LOGGER = LoggerFactory.getLogger(BuildUserLSA.class);

    private static final String RESULTS_PATH = "results-path";
    private static final String TWEETS_PATH = "tweets-path";
    private static final String LSA_PATH = "lsa-path";

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
                .flatMap(new BuildUserIndex.Deserializer());


        DataSet<Tuple2<Long, String>> reducedUserObjects = tweets
                .flatMap(new UserTextExtractor())
                .groupBy(0)
                .reduce(new UserTextCombine())
                .map(new UserTextLSA())
                .withParameters(parameters)
                .setParallelism(4);

        reducedUserObjects
                .output(
                        new PostgresFileSink<Tuple2<Long, String>>("text-lsa").testFile(parameters)
                )
                .withParameters(parameters)
                .setParallelism(2);

        env.execute();
    }

    public static final class UserTextLSA extends RichMapFunction<Tuple2<Long, String>, Tuple2<Long, String>> {

        private static final long serialVersionUID = 1L;

        private transient LSAVectorProvider provider;

        @Override
        public void open(Configuration parameters) throws Exception {
            provider = new LSAVectorProvider(new LSM(parameters.getString("lsa.file", "")+"/X", 100, true));
        }

        @Override
        public Tuple2<Long, String> map(Tuple2<Long, String> value) throws Exception {
            float[] vector = ((DenseVector) provider.toVector(value.f1)).toArray();
            StringBuilder vectorString = new StringBuilder();
            for (float element : vector) {
                if (vectorString.length() > 0) {
                    vectorString.append(',');
                }
                vectorString.append(Float.toString(element));
            }
            return new Tuple2<>(value.f0, vectorString.toString());
        }
    }

    public static final class UserTextCombine implements ReduceFunction<Tuple2<Long, String>> {

        @Override
        public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + " " + value2.f1);
        }
    }

    public static final class UserTextExtractor implements FlatMapFunction<JsonObject, Tuple2<Long, String>>, JsonObjectProcessor {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(JsonObject status, Collector<Tuple2<Long, String>> out) {
            JsonObject userObject = status.getAsJsonObject("user");

            if (userObject == null) {
                return;
            }

            //ID
            Long id = get(userObject, Long.class, "id");
            if (id == null) {
                return;
            }

            String text = get(status, String.class, "text");
            if (text == null) {
                return;
            }

            out.collect(new Tuple2<>(id, text));

            //Recursively process retweeted object
            JsonObject retweetedObject = status.getAsJsonObject("retweeted_status");
            if (retweetedObject != null) {
                this.flatMap(retweetedObject, out);
            }
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved (in this case the db params are not required)", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, LSA_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        BuildUserLSA extractor = new BuildUserLSA();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final Path tweetsPath = new Path(cmd.getOptionValue(TWEETS_PATH, String.class));
            //noinspection ConstantConditions
            final String results = cmd.getOptionValue(RESULTS_PATH, String.class);
            //noinspection ConstantConditions
            final String lsaPath = cmd.getOptionValue(LSA_PATH, String.class);

            final Configuration parameters = new Configuration();
            parameters.setString("db.file", results);
            parameters.setString("lsa.file", lsaPath);

            extractor.startPipeline(tweetsPath, parameters);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
