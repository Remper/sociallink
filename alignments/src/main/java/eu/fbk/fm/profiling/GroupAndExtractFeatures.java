package eu.fbk.fm.profiling;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.pf.PFBuilder;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.profiling.extractors.Extractor;
import eu.fbk.fm.profiling.extractors.Features;
import eu.fbk.fm.profiling.extractors.LSA.LSM;
import eu.fbk.fm.profiling.extractors.TextExtractor;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.math.Vector;
import eu.fbk.utils.mylibsvm.svm_node;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static eu.fbk.fm.alignments.index.db.tables.UserSg.USER_SG;

/**
 * Reads the filtered twitter stream, groups by a person and extracts features
 */
public class GroupAndExtractFeatures implements JsonObjectProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupAndExtractFeatures.class);

    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";
    private static final String RESULTS_PATH = "results-path";
    private static final String LIST_PATH = "list";
    private static final String TWEETS_PATH = "tweets-path";
    private static final String LSA_PATH = "lsa-path";
    private static final Gson GSON = new Gson();

    private final DataSource source;
    private final List<String> uids;
    private final LSM lsa;

    public GroupAndExtractFeatures(DataSource source, String lsaPath, List<String> uids) throws IOException {
        this.source = source;
        this.uids = uids;
        this.lsa = new LSM(lsaPath+"/X", 100, true);
    }

    public static class IdTimedUser implements JsonObjectProcessor {
        public String id;
        public Long timestamp;
        public JsonObject data;

        public static IdTimedUser of(final JsonObject object, final String... path) {
            IdTimedUser result = new IdTimedUser();
            result.data = result.get(object, JsonObject.class, path);
            Long timestamp = result.get(object, Long.class, "timestamp_ms");
            result.timestamp = timestamp == null ? 0 : timestamp;
            result.id = result.get(result.data, String.class, "screen_name");
            return result;
        }

        public static IdTimedUser zero() {
            IdTimedUser result = new IdTimedUser();
            result.data = null;
            result.id = null;
            result.timestamp = -1L;
            return result;
        }

        public IdTimedUser max(IdTimedUser user) {
            IdTimedUser result = new IdTimedUser();
            if (id == null || user.timestamp > timestamp) {
                result.id = user.id;
                result.data = user.data;
                result.timestamp = user.timestamp;
                return result;
            }

            result.id = id;
            result.data = data;
            result.timestamp = timestamp;
            return result;
        }
    }

    public void extractSocialGraph(String outputPath) {
        /*for (String uid : uids) {
            DSL.using(source, SQLDialect.POSTGRES)
                    .select(USER_SG.fields())
                    .from(USER_SG)
                    .where(USER_SG.UID.eq());
        }*/
    }

    public void start(String inputPath, String outputPath) {
        LinkedList<File> files = new LinkedList<>();
        for (File file : new File(inputPath).listFiles()) {
            if (file.getName().startsWith(".") || file.isDirectory()) {
                continue;
            }
            files.add(file);
        }
        LOGGER.info("Files found: " + files.size());

        final ActorSystem system = ActorSystem.create(GroupAndExtractFeatures.class.getSimpleName());
        final Materializer materializer = ActorMaterializer.create(
                ActorMaterializerSettings
                        .create(system)
                        .withSupervisionStrategy(Supervision.resumingDecider())
                , system
        );

        Flow<ByteString, String, NotUsed> lineSplit =
                Framing
                        .delimiter(ByteString.fromString(System.lineSeparator()), Integer.MAX_VALUE, FramingTruncation.ALLOW)
                        .map(ByteString::utf8String);

        Flow<File, JsonObject, NotUsed> tweets =
                Flow.of(File.class)
                        .flatMapMerge(files.size(), file -> FileIO
                                .fromPath(file.toPath())
                                //.via(Compression
                                //        .gunzip(4096)
                                //        .recoverWithRetries(1, new PFBuilder<Throwable, Source<ByteString, NotUsed>>().matchAny(ex -> Source.single(ByteString.empty())).build())
                                //).async()
                                .via(lineSplit).async())
                        .map(tweet -> GSON.fromJson(tweet, JsonObject.class)).async();

        Flow<JsonObject, IdTimedUser, NotUsed> userObjects =
                Flow.of(JsonObject.class)
                        .mapConcat(tweet -> Arrays.asList(
                                IdTimedUser.of(tweet, "user"),
                                IdTimedUser.of(tweet, "retweeted_status", "user")
                        )).async();

        Flow<IdTimedUser, IdTimedUser, NotUsed> filterUsers =
                Flow.of(IdTimedUser.class)
                        .filter(user -> user.data != null && uids.contains(user.id.toLowerCase())).async();

        Flow<IdTimedUser, IdTimedUser, NotUsed> pickLatestUserObject =
                Flow.of(IdTimedUser.class)
                        .groupBy(Integer.MAX_VALUE, user -> user.id).async()
                        .reduce(IdTimedUser::max).async()
                        .mergeSubstreams().async();

        Flow<IdTimedUser, ByteString, NotUsed> serializeForOutput =
                Flow.of(IdTimedUser.class)
                        .map(user -> ByteString.fromString("\n" + user.id + "\t" + user.timestamp + "\t" + GSON.toJson(user.data))).async();

        // Statistics counters
        AtomicInteger withTimestamp = new AtomicInteger();
        AtomicInteger withoutTimestamp = new AtomicInteger();
        AtomicInteger processedUsers = new AtomicInteger();

        // Extracted features
        Extractor[] extractors = new Extractor[]{new TextExtractor(this.lsa, this.uids)};
        HashMap<Extractor, Features> features = new HashMap<>();
        for (Extractor extractor : extractors) {
            features.put(extractor, new Features());
        }

        Source
                .from(files)
                .alsoTo(Sink.foreach(file -> LOGGER.info("File found: " + file.toString())))
                .via(tweets)
                .alsoTo(Sink.foreach(tweet -> {
                    Long timestamp = get(tweet, Long.class, "timestamp_ms");
                    int with = timestamp != null ? withTimestamp.incrementAndGet() : withTimestamp.get();
                    int without = timestamp != null ? withoutTimestamp.get() : withoutTimestamp.incrementAndGet();
                    if ((with + without) % 50000 == 0) {
                        LOGGER.info(String.format("With ts: %4dk Without ts: %4dk", with / 1000, without / 1000));
                    }
                })).async()
                .runForeach(tweet -> {
                    for (Extractor extractor : extractors) {
                        Features feature = features.get(extractor);
                        extractor.extract(tweet, feature);
                    }
                }, materializer)
                /*.via(userObjects)
                .via(filterUsers)
                .via(pickLatestUserObject)
                .alsoTo(Sink.foreach(file -> processedUsers.incrementAndGet())).async()
                .via(serializeForOutput)
                .runWith(FileIO.toPath(new File(outputPath, "users.json").toPath()), materializer)*/
                .whenComplete((ioResult, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Something happened during the execution", throwable);
                    }
                    LOGGER.info(String.format("Processed %d tweets", withoutTimestamp.get() + withTimestamp.get()));
                    dumpExtractors(outputPath, features);
                    system.terminate();
                });
    }

    private void dumpExtractors(String outputPath, HashMap<Extractor, Features> features) {
        for (Extractor extractor : features.keySet()) {
            try {
                Collection<Features.FeatureSet> feature = features.get(extractor).getFeatures();
                LOGGER.info(String.format("Users for extractor %s: %d", extractor.getId(), feature.size()));
                LOGGER.info("  "+extractor.statsString());

                Files
                    .asCharSink(new File(outputPath, extractor.getId()+".svm"), Charsets.UTF_8)
                    .writeLines(
                        feature
                            .stream()
                            .map(user -> user.name + " " + svm_node.toString(((Vector) user.features).toSvmNodeArray()))
                    );
            } catch (IOException e) {
                LOGGER.error("Error happened while dumping users for extractor "+extractor.getId(), e);
            }
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("c", DB_CONNECTION,
                        "connection string for the database", "DB",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_USER,
                        "user for the database", "USER",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_PASSWORD,
                        "password for the database", "PASSWORD",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false)
                .withOption("l", LIST_PATH,
                        "specifies the file with the list of user handlers to filter", "FILE",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, LSA_PATH,
                        "Location of the LSA model", "FILE",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final String dbConnection = cmd.getOptionValue(DB_CONNECTION, String.class);
            //noinspection ConstantConditions
            final String dbUser = cmd.getOptionValue(DB_USER, String.class);
            //noinspection ConstantConditions
            final String dbPassword = cmd.getOptionValue(DB_PASSWORD, String.class);

            //noinspection ConstantConditions
            final String listPath = cmd.getOptionValue(LIST_PATH, String.class);
            //noinspection ConstantConditions
            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            //noinspection ConstantConditions
            final String lsaPath = cmd.getOptionValue(LSA_PATH, String.class);
            final String resultsPath;
            if (cmd.hasOption(RESULTS_PATH)) {
                resultsPath = cmd.getOptionValue(RESULTS_PATH, String.class);
            } else {
                resultsPath = tweetsPath;
            }

            List<String> uids = Files.asCharSource(new File(listPath), Charsets.UTF_8)
                    .readLines().stream()
                    .map(line -> line.split(",")[1].toLowerCase())
                    .collect(Collectors.toList());
            LOGGER.info(String.format("Loaded %d uids", uids.size()));

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);

            GroupAndExtractFeatures extractor = new GroupAndExtractFeatures(source, lsaPath, uids);
            extractor.start(tweetsPath, resultsPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
