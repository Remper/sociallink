package eu.fbk.fm.profiling;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Procedure;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import eu.fbk.fm.profiling.extractors.*;
import eu.fbk.fm.profiling.extractors.LSA.LSM;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.math.SparseVector;
import eu.fbk.utils.math.Vector;
import eu.fbk.utils.mylibsvm.svm_node;
import org.jooq.Record2;
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
    private static final String UID_LIST_PATH = "uid-list";
    private static final String TWEETS_PATH = "tweets-path";
    private static final String LSA_PATH = "lsa-path";
    private static final Gson GSON = new Gson();

    private final DataSource source;
    private final ImmutableSet<String> uids;
    private final LSM lsa;

    public GroupAndExtractFeatures(DataSource source, String lsaPath, ImmutableSet<String> uids) throws IOException {
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

    public void extractSocialGraph(Map<String, Long> uidMapping, String outputPath) {
        HashMap<Long, Integer> sgMapping = new HashMap<>();
        List<Features.FeatureSet> features = new LinkedList<>();
        int counter = 0;
        for (Map.Entry<String, Long> user : uidMapping.entrySet()) {
            Record2<Long[], Float[]> userVectorRaw =
                DSL.using(source, SQLDialect.POSTGRES)
                    .select(USER_SG.FOLLOWEES, USER_SG.WEIGHTS)
                    .from(USER_SG)
                    .where(USER_SG.UID.eq(user.getValue()))
                    .fetchOne();

            counter++;
            if (counter % 4000 == 0) {
                LOGGER.info("  [social_graph] processed "+counter+" users, added "+features.size());
            }

            if (userVectorRaw == null) {
                //LOGGER.warn(String.format("User %s (%d) hasn't been found", user.getKey(), user.getValue()));
                continue;
            }

            SparseVector vector = new SparseVector();
            for (int i = 0; i < userVectorRaw.value1().length; i++) {
                Long curId = userVectorRaw.value1()[i];
                Float curWeight = userVectorRaw.value2()[i];

                if (!sgMapping.containsKey(curId)) {
                    sgMapping.put(curId, sgMapping.size());
                }
                int remappedId = sgMapping.get(curId);

                vector.set(remappedId, curWeight);
            }
            features.add(new Features.FeatureSet<>(user.getKey(), "social_graph", vector, 0L));
        }

        try {
            Files
                .asCharSink(new File(outputPath, "social_graph.dict"), Charsets.UTF_8)
                .writeLines(
                    sgMapping.entrySet().stream()
                        .map(entry -> entry.getKey() + "\t" + entry.getValue())
                );
        } catch (IOException e) {
            LOGGER.error("Error happened while dumping dictionary for extractor social_graph", e);
        }
        dumpFeatures(features, "social_graph", outputPath);
    }

    public void start(String inputPath, String outputPath) {
        final int factor = 5;
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

        Flow<String, JsonObject, NotUsed> deserialize =
                Flow.fromGraph(GraphDSL.create(b -> {
                    final UniformFanInShape<JsonObject, JsonObject> mergeDeserialization =
                            b.add(Merge.create(factor));
                    final UniformFanOutShape<String, String> dispatchDeserialization =
                            b.add(Balance.create(factor));
                    final Flow<String, JsonObject, NotUsed> deserializator =
                            Flow.of(String.class).map(tweet -> GSON.fromJson(tweet, JsonObject.class));

                    for (int i=0; i<factor; i++) {

                        b.from(dispatchDeserialization.out(i))
                            .via(b.add(deserializator.async()))
                            .toInlet(mergeDeserialization.in(i));
                    }

                    return FlowShape.of(dispatchDeserialization.in(), mergeDeserialization.out());
                }));

        Flow<File, JsonObject, NotUsed> tweets =
                Flow.of(File.class)
                        .flatMapMerge(files.size(), file -> FileIO
                                .fromPath(file.toPath())
                                //.via(Compression
                                //        .gunzip(4096)
                                //        .recoverWithRetries(1, new PFBuilder<Throwable, Source<ByteString, NotUsed>>().matchAny(ex -> Source.single(ByteString.empty())).build())
                                //).async()
                                .via(lineSplit).async()).async()
                        .via(deserialize).async();

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
        AtomicInteger processedTweets = new AtomicInteger();

        // Extracted features
        HashtagExtractor hashtagExtractor = new HashtagExtractor(this.uids);
        ProfileExtractor profileExtractor = new ProfileExtractor(this.lsa, this.uids);
        Extractor[] extractors = new Extractor[]{
            new TextExtractor(this.lsa, this.uids),
            new MentionedTextExtractor(this.lsa, this.uids),
            new MentionedTextExtractor.MentionedTextExtractorLSA(this.lsa, this.uids),
            new TextExtractor.TextExtractorLSA(this.lsa, this.uids),
            hashtagExtractor,
            profileExtractor
        };
        HashMap<Extractor, Features> features = new HashMap<>();
        for (Extractor extractor : extractors) {
            features.put(extractor, new Features());
        }
        LOGGER.info(String.format("Initialised %d extractors", extractors.length));

        Flow<JsonObject, Boolean, NotUsed> featureExtractor =
            Flow.fromGraph(GraphDSL.create(b -> {
                final UniformFanInShape<Boolean, Boolean> mergeExtractor =
                        b.add(Merge.create(extractors.length*factor));
                final UniformFanOutShape<JsonObject, JsonObject> dispatchExtractors =
                        b.add(Broadcast.create(extractors.length));
                final List<UniformFanOutShape<JsonObject, JsonObject>> dispatchByFactor = new ArrayList<>(extractors.length);
                for (Extractor extractor : extractors) {
                    dispatchByFactor.add(b.add(Balance.create(factor)));
                }

                for (int i=0; i<extractors.length; i++) {
                    final int index = i;
                    final Features feature = features.get(extractors[index]);
                    final Flow<JsonObject, Boolean, NotUsed> extractor = Flow.of(JsonObject.class)
                        .map(tweet -> {
                            extractors[index].extract(tweet, feature);
                            return true;
                        });

                    b.from(dispatchExtractors.out(i))
                        .toInlet(dispatchByFactor.get(i).in());

                    for (int j=0; j<factor; j++) {
                        b.from(dispatchByFactor.get(i).out(j))
                            .via(b.add(extractor.async()))
                            .toInlet(mergeExtractor.in(i*factor+j));
                    }
                }

                return FlowShape.of(dispatchExtractors.in(), mergeExtractor.out());
            }));

        Source
            .from(files)
            .alsoTo(Sink.foreach(file -> LOGGER.info("File found: " + file.toString())))
            .via(tweets)
            .alsoTo(Sink.foreach(tweet -> {
                Long timestamp = get(tweet, Long.class, "timestamp_ms");
                int with = timestamp != null ? withTimestamp.incrementAndGet() : withTimestamp.get();
                int without = timestamp != null ? withoutTimestamp.get() : withoutTimestamp.incrementAndGet();
                if ((with + without) % 50000 == 0) {
                    int withOutput = with / 1000;
                    String withSymbol = "k";
                    if (with > 1000000) {
                        withOutput = with / 1000000;
                        withSymbol = "m";
                    }
                    LOGGER.info(String.format("With ts: %4d%s Without ts: %4dk", withOutput, withSymbol, without / 1000));
                }
            })).async()
            .via(featureExtractor).async()
            .runForeach(res -> {
                int processed = processedTweets.incrementAndGet();
                if (processed % (400000*extractors.length) == 0) {
                    LOGGER.info(String.format("Processed %.1fm tweets by %d extractors", ((float) processed / extractors.length) / 1000000, extractors.length));
                    for (Map.Entry<Extractor, Features> entry : features.entrySet()) {
                        LOGGER.info(String.format("  [%s] dict size: %d", entry.getKey().getId(), entry.getValue().getSize()));
                    }
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
                LOGGER.info("Dumping hashtag dictionary");
                dumpHashtagExtractorToFile(outputPath, hashtagExtractor);
                LOGGER.info("Dumping profile dictionary");
                dumpProfileExtractorToFile(outputPath, profileExtractor);

                system.terminate();
            });
    }

    private void dumpHashtagExtractorToFile(String outputPath, HashtagExtractor extractor) {
        try {
            Files
                .asCharSink(new File(outputPath, extractor.getId()+".dict"), Charsets.UTF_8)
                .writeLines(
                    extractor.getDictionary().map(HashtagExtractor.DictTerm::toString)
                );
        } catch (IOException e) {
            LOGGER.error("Error happened while dumping dictionary for extractor "+extractor.getId(), e);
        }
    }

    private void dumpProfileExtractorToFile(String outputPath, ProfileExtractor extractor) {
        try {
            Files
                    .asCharSink(new File(outputPath, extractor.getId()+".lang.dict"), Charsets.UTF_8)
                    .writeLines(extractor.getDictionary());
        } catch (IOException e) {
            LOGGER.error("Error happened while dumping dictionary for extractor "+extractor.getId(), e);
        }
    }

    private void dumpExtractors(String outputPath, HashMap<Extractor, Features> features) {
        for (Extractor extractor : features.keySet()) {
            Collection<Features.FeatureSet> feature = features.get(extractor).getFeatures();
            LOGGER.info(String.format("Users for extractor %s: %d", extractor.getId(), feature.size()));
            LOGGER.info("  "+extractor.statsString());

            dumpFeatures(feature, extractor.getId(), outputPath);
        }
    }

    private void dumpFeatures(Collection<Features.FeatureSet> features, String extractorId, String outputPath) {
        try {
            Files
                .asCharSink(new File(outputPath, extractorId+".svm"), Charsets.UTF_8)
                .writeLines(
                    features
                        .stream()
                        .map(user -> user.name + " " + svm_node.toString(((Vector) user.features).toSvmNodeArray()))
                );
        } catch (IOException e) {
            LOGGER.error("Error happened while dumping users for extractor "+extractorId, e);
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
                .withOption(null, UID_LIST_PATH,
                        "file with screen_name -> uid mapping", "FILE",
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
            final String uidListPath = cmd.getOptionValue(UID_LIST_PATH, String.class);
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

            ImmutableSet<String> uids = ImmutableSet.<String>builder().addAll(
                Files.asCharSource(new File(listPath), Charsets.UTF_8)
                    .readLines().stream()
                    .map(line -> line.split(",")[1].toLowerCase())
                    .collect(Collectors.toList())
            ).build();
            LOGGER.info(String.format("Loaded %d uids", uids.size()));

            Map<String, Long> uidMapping = new HashMap<>();
            Files.asCharSource(new File(uidListPath), Charsets.UTF_8)
                    .readLines().stream()
                    .map(line -> line.split("\t"))
                    .forEach(line -> {
                        if (line.length < 2) {
                            LOGGER.error("Faulty line: "+line[0]);
                            return;
                        }
                        uidMapping.put(line[0].toLowerCase(), Long.valueOf(line[1]));
                    });
            LOGGER.info(String.format("Loaded %d uid mappings", uidMapping.size()));

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);

            GroupAndExtractFeatures extractor = new GroupAndExtractFeatures(source, lsaPath, uids);
            //extractor.extractSocialGraph(uidMapping, resultsPath);
            extractor.start(tweetsPath, resultsPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
