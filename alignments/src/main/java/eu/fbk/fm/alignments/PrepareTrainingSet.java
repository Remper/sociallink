package eu.fbk.fm.alignments;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import eu.fbk.fm.alignments.evaluation.CustomEvaluation;
import eu.fbk.fm.alignments.evaluation.Dataset;
import eu.fbk.fm.alignments.evaluation.DatasetEntry;
import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.persistence.sparql.InMemoryEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.ResourceEndpoint;
import eu.fbk.fm.alignments.query.*;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.*;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.MemoryEmbeddingsProvider;
import eu.fbk.fm.alignments.scorer.text.VectorProvider;
import eu.fbk.fm.alignments.twitter.SearchRunner;
import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import eu.fbk.fm.alignments.twitter.TwitterService;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.Scaler;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullWriter;
import org.jooq.Record2;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static eu.fbk.fm.alignments.index.db.tables.UserIndex.USER_INDEX;
import static eu.fbk.fm.alignments.index.db.tables.UserObjects.USER_OBJECTS;
import static eu.fbk.fm.alignments.scorer.TextScorer.DBPEDIA_TEXT_EXTRACTOR;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.sum;

/**
 * Script that evaluates a particular alignments pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class PrepareTrainingSet {

    public static final int CANDIDATES_THRESHOLD = 40;
    public static final int RESOLVE_CHUNK_SIZE = 2000;
    private static final Gson GSON = TwitterDeserializer.getDefault().getBuilder().create();
    private static final Logger logger = LoggerFactory.getLogger(PrepareTrainingSet.class);
    private Endpoint endpoint;
    private TwitterCredentials[] credentials = null;
    private TwitterService service = null;
    private QueryAssemblyStrategy qaStrategy = QueryAssemblyStrategyFactory.def();
    private ScoringStrategy scoreStrategy = null;

    private FillFromIndex index = null;

    public PrepareTrainingSet(Endpoint endpoint) throws Exception {
        Objects.requireNonNull(endpoint);
        this.endpoint = endpoint;
    }

    public PrepareTrainingSet(FillFromIndex index) throws Exception {
        Objects.requireNonNull(index);
        this.index = index;
    }

    public static void dumpStats(List<FullyResolvedEntry> entries, CSVPrinter statsPrinter) {
        try {
            for (FullyResolvedEntry entry : entries) {
                for (UserData user : entry.candidates) {
                    boolean isPositive = user.getScreenName().equalsIgnoreCase(entry.entry.twitterId);

                    if (!isPositive) {
                        continue;
                    }
                    statsPrinter.printRecord(
                            entry.resource.getIdentifier(),
                            user.getScreenName(),
                            isPositive ? 1 : 0,
                            entry.resource.isCompany() ? "org" : "per",
                            user.getFollowersCount(),
                            user.get("frequency").map(JsonElement::getAsInt).orElse(0),
                            user.isVerified() ? 1 : 0
                    );
                }
            }
        } catch (IOException e) {
            logger.error("Error while dumping stats", e);
        }
    }

    private static void strategiesCheck(List<FullyResolvedEntry> resolveDataset, File file) {
        //Here we check all the strategies doing a random pick from the resolved dataset
        QueryAssemblyStrategy[] strategies = {
            new AllNamesStrategy(),
            new NoQuotesDupesStrategy(),
            new StrictQuotesStrategy(),
            new StrictStrategy(),
            new StrictWithTopicStrategy()
        };

        Random rnd = new Random();
        try (FileWriter writer = new FileWriter(file)) {
            for (int i = 0; i < 10; i++) {
                FullyResolvedEntry entry = resolveDataset.get(rnd.nextInt(resolveDataset.size()));

                writer.write(String.format("Entity: %s. True alignment: @%s\n", entry.entry.resourceId, entry.entry.twitterId));
                writer.write(String.format("  Names: %s\n", String.join(", ", entry.resource.getNames())));
                writer.write(String.format("  Labels: %s\n", String.join(", ", entry.resource.getLabels())));

                for (QueryAssemblyStrategy strategy : strategies) {
                    writer.write(String.format("  Strategy: %s. Query: %s\n", strategy.getClass().getSimpleName(), strategy.getQuery(entry.resource)));
                }

                if (entry.candidates == null || entry.candidates.size() == 0) {
                    writer.write("  No candidates");
                    continue;
                }

                writer.write("  Candidates:\n");
                for (User candidate : entry.candidates) {
                    writer.write(String.format("    @%s (%s)\n", candidate.getScreenName(), candidate.getName()));
                }
            }
        } catch (IOException e) {
            logger.warn("Error while performing strategy check", e);
        }
        logger.info("Strategies check finished");
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = loadConfiguration(args);
        if (configuration == null) {
            return;
        }

        if (configuration.dbConnection == null || configuration.dbUser == null || configuration.dbPassword == null) {
            logger.error("DB credentials are not specified");
            return;
        }

        logger.info(String.format("Options %s", GSON.toJson(configuration)));

        QueryAssemblyStrategy qaStrategy = new AllNamesStrategy();//QueryAssemblyStrategyFactory.get(configuration.strategy);

        ResourceEndpoint endpoint = configuration.createEndpoint();
        Gson gson = TwitterDeserializer.getDefault().getBuilder().create();
        PrepareTrainingSet prepareTrainingSet;

        DataSource source = DBUtils.createHikariDataSource(configuration.dbConnection, configuration.dbUser, configuration.dbPassword);
        //if (configuration.credentials == null) {
        prepareTrainingSet = new PrepareTrainingSet(new FillFromIndex(endpoint, qaStrategy, source));
        //} else {
        //    prepareTrainingSet = new PrepareTrainingSet(endpoint);
        //    prepareTrainingSet.setQAStrategy(new StrictStrategy());
        //}

        if (configuration.lsa == null) {
            logger.info("LSA is not specified. Stopping");
            return;
        }

        PAI18Strategy strategy = new PAI18Strategy(source);
        LSM lsm = new LSM(configuration.lsa + "/X", 100, true);
        VectorProvider textVectorProvider = new LSAVectorProvider(lsm);
        List<VectorProvider> allVectorProviders = new LinkedList<>();
        allVectorProviders.add(textVectorProvider);
        if (configuration.embeddings != null) {
            LinkedList<VectorProvider> embProviders = new LinkedList<>();
            Files.list(Paths.get(configuration.embeddings)).forEach((path) -> {
                try {
                    embProviders.add(new MemoryEmbeddingsProvider(path.toString(), configuration.lsa));
                } catch (Exception e) {
                    logger.error("Error while loading embedding", e);
                }
            });
            logger.info("Loaded {} embedding models", embProviders.size());
            allVectorProviders.addAll(embProviders);
        }
        for (VectorProvider provider : allVectorProviders) {
            strategy.addProvider(ISWC17Strategy.builder().source(source).vectorProvider(provider).build());
        }
        if (allVectorProviders.size() > 1) {
            strategy.addProvider(ISWC17Strategy.builder().source(source).vectorProviders(allVectorProviders).build());
        }
        //logger.info("LSA specified. Enabling PAI18 strategy");
        prepareTrainingSet.setScoreStrategy(strategy);
        //logger.info("LSA specified. Enabling SMT strategy");
        //prepareTrainingSet.setScoreStrategy(new PAI18Strategy(new LinkedList<>()));
        //prepareTrainingSet.setScoreStrategy(new SMTStrategy(source, configuration.lsa));

        FileProvider files = new FileProvider(configuration.workdir);
        if (!files.gold.exists()) {
            logger.error("Gold standard dataset doesn't exist");
            return;
        }

        //Loading full gold standard
        Dataset goldStandardDataset = Dataset.fromFile(files.gold);

        if (configuration.credentials != null) {
            prepareTrainingSet.setCredentials(TwitterCredentials.credentialsFromFile(new File(configuration.credentials)));
        }

        //Resolving all the data needed for analysis
        int curLastIndex = -1;
        int totalChunks = (int) Math.ceil((float) goldStandardDataset.size() / RESOLVE_CHUNK_SIZE);
        if (files.resolved.exists()) {
            File[] resolveChunks = files.resolved.listFiles();
            if (resolveChunks == null) {
                logger.error("Something wrong with the resolve directory");
                return;
            }

            for (File file : resolveChunks) {
                if (!file.getName().endsWith(".gz")) {
                    continue;
                }

                int index = Integer.valueOf(file.getName().substring(0, file.getName().indexOf('.')));
                if (index > curLastIndex) {
                    curLastIndex = index;
                }
            }
        } else {
            if (!files.resolved.mkdir()) {
                logger.error("Can't create directory for the resolved info");
                return;
            }
        }
        curLastIndex++;

        if (curLastIndex < totalChunks) {
            int toSkip = curLastIndex;

            logger.info(String.format(
                    "Resolving all data from the dataset (%d entries, skip chunks: %d, projected chunks: %d)",
                    goldStandardDataset.size(), toSkip, totalChunks
            ));
            logger.info("Query strategy: " + prepareTrainingSet.getQaStrategy().getClass().getSimpleName());

            Dataset curDataset = new Dataset();
            for (DatasetEntry entry : goldStandardDataset) {
                curDataset.add(entry);
                if (curDataset.size() > RESOLVE_CHUNK_SIZE) {
                    if (toSkip == 0) {
                        prepareTrainingSet.resolveAndSaveDatasetChunk(curDataset, curLastIndex, files);
                        curLastIndex++;
                    } else {
                        toSkip--;
                    }
                    curDataset = new Dataset();
                }
            }
            if (curDataset.size() > 0) {
                prepareTrainingSet.resolveAndSaveDatasetChunk(curDataset, curLastIndex, files);
            }
        }

        // Deserializing the entire collection
        final List<FullyResolvedEntry> resolveDataset = new LinkedList<>();
        logger.info("Deserialising user data and generating features");
        List<File> resolveChunks = Arrays.asList(Optional.ofNullable(files.resolved.listFiles()).orElse(new File[0]));
        if (resolveChunks.size() == 0) {
            logger.error("Can't deserialize user data");
            return;
        }
        AtomicInteger chunksLeft = new AtomicInteger(resolveChunks.size());
        CSVPrinter statsPrinter = new CSVPrinter(new FileWriter(files.datasetStats), CSVFormat.TDF);
        resolveChunks.forEach(file -> {
            Stopwatch watch = Stopwatch.createStarted();
            Reader reader;
            try {
                reader = new InputStreamReader(new GZIPInputStream(new FileInputStream(file)));
            } catch (IOException e) {
                throw new RuntimeException("Can't open file " + file);
            }
            Type type = new TypeToken<List<FullyResolvedEntry>>() {
            }.getType();
            List<FullyResolvedEntry> entries = gson.fromJson(reader, type);
            logger.info(String.format(
                    "Chunk %s deserialized in %.2f seconds",
                    file.getName(),
                    (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000
            ));
            watch.reset().start();
            prepareTrainingSet.generateFeatures(entries);
            dumpStats(entries, statsPrinter);
            prepareTrainingSet.purgeAdditionalData(entries);
            resolveDataset.addAll(entries);
            IOUtils.closeQuietly(reader);
            logger.info(String.format(
                    "Chunk %s completed in %.2f seconds (left %2d)",
                    file.getName(),
                    (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000,
                    chunksLeft.decrementAndGet()
            ));
        });
        //statsPrinter.close();

        strategiesCheck(resolveDataset, files.strategyCheck);

        try {
            Set<String> resourceIds = new HashSet<>();
            int numCandidates = 0;
            int numNoCandidates = 0;
            int trueCandidates = 0;
            int maxAmountOfCandidates = 0;
            int longTail = 5;
            int[] cutoff = new int[]{0, 0};
            int[] indexLoss = new int[]{0, 0};
            for (FullyResolvedEntry entry : resolveDataset) {
                if (maxAmountOfCandidates < entry.candidates.size()) {
                    maxAmountOfCandidates = entry.candidates.size();
                }
            }
            int[] trueCandidatesOrder = new int[maxAmountOfCandidates];
            int[] distributionCandidates = new int[maxAmountOfCandidates + 1];
            Map<String, List<FullyResolvedEntry>> missingTrueAlignments = new HashMap<>();
            for (FullyResolvedEntry entry : resolveDataset) {
                if (entry == null) {
                    logger.error("Entry is null for some reason!");
                    continue;
                }
                numCandidates += entry.candidates.size();
                if (entry.candidates.size() == 0) {
                    numNoCandidates++;
                }
                distributionCandidates[entry.candidates.size()] += 1;
                int order = 0;
                boolean foundTrue = false;
                for (User candidate : entry.candidates) {
                    if (candidate.getScreenName().equalsIgnoreCase(entry.entry.twitterId)) {
                        trueCandidates++;
                        trueCandidatesOrder[order]++;
                        if (order >= 40) {
                            cutoff[entry.resource.isCompany() ? 1 : 0] += 1;
                        }
                        if (order > 50 && longTail > 0) {
                            logger.info(String.format("   An alignment within the long tail (%d): %s", order + 1, entry.entry.resourceId));
                            longTail--;
                        }
                        foundTrue = true;
                        break;
                    }
                    order++;
                }
                if (!foundTrue) {
                    indexLoss[entry.resource.isCompany() ? 1 : 0] += 1;
                    String twitterId = entry.entry.twitterId.toLowerCase();
                    if (!missingTrueAlignments.containsKey(twitterId)) {
                        missingTrueAlignments.put(twitterId, new LinkedList<>());
                    }
                    missingTrueAlignments.get(twitterId).add(entry);
                }
                if (resourceIds.contains(entry.entry.resourceId)) {
                    continue;
                }
                resourceIds.add(entry.entry.resourceId);
            }
            logger.info("Dataset statistics: ");
            logger.info(" Items before resolving:\t" + goldStandardDataset.size());
            logger.info(" Items after resolving:\t" + resolveDataset.size());
            int lost = goldStandardDataset.size() - resolveDataset.size();
            logger.info(String.format(" Lost:\t%d (%.2f", lost, ((double) lost / goldStandardDataset.size()) * 100) + "%)");
            logger.info(String.format(" Average candidates per entity: %.2f", (double) numCandidates / resolveDataset.size()));
            logger.info(String.format(" Entities without candidates: %d (%.2f", numNoCandidates, ((double) numNoCandidates / resolveDataset.size()) * 100) + "%)");
            logger.info(String.format(" Entities with true candidate: %d (%.2f", trueCandidates, ((double) trueCandidates / resolveDataset.size()) * 100) + "%)");

            List<String> trueDist = new LinkedList<>();
            List<String> totalDist = new LinkedList<>();
            int candSum = 0;
            for (int i = 0; i < trueCandidatesOrder.length; i++) {
                candSum += trueCandidatesOrder[i];
                trueDist.add(String.format("%d\t%d\t%.4f", i, candSum, ((double) candSum) / resolveDataset.size()));
            }
            for (int i = 0; i < distributionCandidates.length; i++) {
                totalDist.add(String.format("%d\t%d", i, distributionCandidates[i]));
            }
            logger.info(String.format(" CA Recall: %.2f%%", ((double) candSum / resolveDataset.size()) * 100));
            int cutoffSum = cutoff[0] + cutoff[1];
            int indexLossSum = indexLoss[0] + indexLoss[1];
            logger.info(String.format(" CA Index loss: %d (%.2f%%, persons: %d, organisations: %d)", indexLossSum, ((double) indexLossSum / resolveDataset.size()) * 100, indexLoss[0], indexLoss[1]));
            logger.info(String.format(" CA Cutoff loss: %d (%.2f%%, persons: %d, organisations: %d)", cutoffSum, ((double) cutoffSum / resolveDataset.size()) * 100, cutoff[0], cutoff[1]));
            Files.write(Paths.get(files.trueDist.getAbsolutePath()), trueDist);
            Files.write(Paths.get(files.totalDist.getAbsolutePath()), totalDist);
            totalDist.clear();
            totalDist.clear();

            // Temporary part of pipeline that resolves IDs of missing profiles and tries to resolve them against DB
            if (configuration.userDictionary != null) {
                // Resolving Ids
                HashMap<String, Long> resolvedIds = new HashMap<>();
                int row = 0;
                int resolvedOrg = 0;
                int resolvedPer = 0;
                try (CSVParser reader = new CSVParser(new InputStreamReader(new GZIPInputStream(new FileInputStream(configuration.userDictionary))), CSVFormat.TDF)) {
                    for (CSVRecord record : reader) {
                        String currentScreenName = record.get(1).toLowerCase();
                        if (missingTrueAlignments.containsKey(currentScreenName)) {
                            for (FullyResolvedEntry entry : missingTrueAlignments.get(currentScreenName)) {
                                if (entry.resource.isCompany()) {
                                    resolvedOrg++;
                                } else {
                                    resolvedPer++;
                                }
                            }
                            resolvedIds.put(currentScreenName, Long.valueOf(record.get(0)));
                        }
                        row++;
                        if (row % 10000000 == 0) {
                            logger.info(String.format("  Read %d0m users from dictionary", row / 10000000));
                        }
                    }
                }
                int streamLoss = missingTrueAlignments.size() - resolvedIds.size();
                logger.info(String.format(" Stream loss: %d (%.2f%%, recovered %d persons, %d organisations)", streamLoss, ((double) streamLoss / resolveDataset.size()) * 100, resolvedPer, resolvedOrg));

                // Executing awesome queries
                prepareTrainingSet.initService();
                logger.info(String.format("Resolving the rest of gold alignments (%d to go)", resolvedIds.size()));
                AtomicInteger nulls = new AtomicInteger(0);
                AtomicInteger resolvedNulls = new AtomicInteger(0);
                resolvedIds.entrySet().parallelStream().map(entry -> {
                    Table<Record2<Long, BigDecimal>> subquery =
                            select(USER_INDEX.UID, sum(USER_INDEX.FREQ).as("freq"))
                                    .from(USER_INDEX)
                                    .where(USER_INDEX.UID.eq(entry.getValue()))
                                    .groupBy(USER_INDEX.UID)
                                    .asTable("a");

                    Optional<Record2> result = Optional.ofNullable(
                            DSL.using(source, SQLDialect.POSTGRES)
                                    .select(USER_OBJECTS.OBJECT, subquery.field("freq"))
                                    .from(subquery)
                                    .join(USER_OBJECTS)
                                    .on(subquery.field(USER_INDEX.UID).eq(USER_OBJECTS.UID))
                                    .fetchOne()
                    );

                    try {
                        UserData data = result.map(res -> {
                            try {
                                UserData userData = new UserData(TwitterObjectFactory.createUser(res.get(USER_OBJECTS.OBJECT).toString()));
                                userData.submitData("frequency", res.get(subquery.field("freq")));
                                return userData;
                            } catch (TwitterException e) {
                                logger.error("Error while deserializing user object", e);
                            }
                            return null;
                        }).orElseGet(() -> {
                            try {
                                return new UserData(prepareTrainingSet.service.getProfile(entry.getValue()));
                            } catch (TwitterService.RateLimitException e) {
                                logger.error("Rate limit exception", e);
                            }
                            return null;
                        });
                        if (!data.getScreenName().equalsIgnoreCase(entry.getKey())) {
                            if (!missingTrueAlignments.containsKey(data.getScreenName().toLowerCase())) {
                                logger.error("Creating a mapping from : @" + entry.getKey() + " -> @" + data.getScreenName());
                                missingTrueAlignments.put(data.getScreenName().toLowerCase(), missingTrueAlignments.get(entry.getKey().toLowerCase()));
                            }
                        }
                        resolvedNulls.incrementAndGet();
                        return data;
                    } catch (Exception e) {
                        nulls.incrementAndGet();
                        return null;
                    }
                }).forEach(userData -> {
                    if (userData == null) {
                        return;
                    }

                    synchronized (prepareTrainingSet) {
                        UserData user = (UserData) userData;
                        List<FullyResolvedEntry> entries = missingTrueAlignments.get(user.getScreenName().toLowerCase());
                        if (entries == null) {
                            logger.error("Something bad happened for screenname: " + user.getScreenName());
                            return;
                        }
                        try {
                            for (FullyResolvedEntry entry : entries) {
                                entry.entry.twitterId = user.getScreenName();
                                statsPrinter.printRecord(
                                    entry.entry.resourceId,
                                    user.getScreenName(),
                                    1,
                                    entry.resource.isCompany() ? "org" : "per",
                                    user.getFollowersCount(),
                                    user.get("frequency").map(JsonElement::getAsInt).orElse(0),
                                    user.isVerified() ? 1 : 0
                                );
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                logger.info("  Unresolvable users: " + nulls.get());
                logger.info("  Recovered users: " + resolvedNulls.get());
            }
            statsPrinter.close();

            Stopwatch watch = Stopwatch.createStarted();
            logger.info("Dumping full experimental setting to JSON");
            FileWriter resolvedWriter = new FileWriter(files.dataset);
            FileWriter filteredGoldWriter = new FileWriter(files.goldFiltered);
            boolean first = true;
            for (FullyResolvedEntry entry : resolveDataset) {
                if (first) {
                    first = false;
                    filteredGoldWriter.write("entity,twitter_id\n");
                } else {
                    resolvedWriter.write('\n');
                    filteredGoldWriter.write('\n');
                }
                filteredGoldWriter.write(String.format("%s,%s", entry.entry.resourceId, entry.entry.twitterId));
                gson.toJson(entry, resolvedWriter);
            }
            IOUtils.closeQuietly(resolvedWriter);
            IOUtils.closeQuietly(filteredGoldWriter);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
            //evaluationPipeline(prepareTrainingSet, files, resolvedTestSet, new ModelEndpoint("localhost", configuration.modelPort));
        } catch (Exception e) {
            logger.error("Error while processing pipeline", e);
            e.printStackTrace();
        }
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("e").desc("Url or path to SPARQL endpoint")
                        .required().hasArg().argName("file").longOpt("endpoint").build()
        );
        options.addOption(
                Option.builder().desc("Database connection string")
                        .required().hasArg().argName("string").longOpt("db-connection").build()
        );
        options.addOption(
                Option.builder().desc("Database user")
                        .required().hasArg().argName("user").longOpt("db-user").build()
        );
        options.addOption(
                Option.builder("p").desc("Database password")
                        .required().hasArg().argName("pass").longOpt("db-password").build()
        );
        options.addOption(
                Option.builder("w").desc("Working directory")
                        .required().hasArg().argName("dir").longOpt("workdir").build()
        );
        options.addOption(
                Option.builder("c").desc("Twitter credentials")
                        .hasArg().argName("credentials").longOpt("credentials").build()
        );
        options.addOption(
                Option.builder("s").desc("Query assembly strategy")
                        .hasArg().argName("strategy").longOpt("strategy").build()
        );
        options.addOption(
                Option.builder().desc("Use LSA")
                        .hasArg().argName("DIRECTORY").longOpt("lsa-path").build()
        );
        options.addOption(
                Option.builder().desc("Path to embeddings to use along with LSA")
                        .hasArg().argName("DIRECTORY").longOpt("embeddings-path").build()
        );
        options.addOption(
                Option.builder().desc("Path to user dictionary needed for additional analysis")
                        .hasArg().argName("DIRECTORY").longOpt("user-dictionary").build()
        );
        options.addOption(
                Option.builder().desc("Port for the model endpoint")
                        .hasArg().argName("PORT").longOpt("model-port").build()
        );

        options.addOption(Option.builder().desc("trace mode").longOpt("trace").build());
        options.addOption(Option.builder().desc("debug mode").longOpt("debug").build());
        options.addOption(Option.builder().desc("info mode").longOpt("info").build());

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            Configuration configuration = new Configuration();
            configuration.endpoint = line.getOptionValue("endpoint");
            configuration.dbConnection = line.getOptionValue("db-connection");
            configuration.dbPassword = line.getOptionValue("db-password");
            configuration.dbUser = line.getOptionValue("db-user");
            configuration.workdir = line.getOptionValue("workdir");
            configuration.credentials = line.getOptionValue("credentials");
            configuration.strategy = line.getOptionValue("strategy");
            configuration.lsa = line.getOptionValue("lsa-path");
            configuration.embeddings = line.getOptionValue("embeddings-path");
            configuration.userDictionary = line.getOptionValue("user-dictionary");
            if (line.hasOption("model-port")) {
                configuration.modelPort = Integer.valueOf(line.getOptionValue("model-port"));
            }

            return configuration;
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed: " + exp.getMessage() + "\n");
            printHelp(options);
        }

        return null;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                200,
                "java -Dfile.encoding=UTF-8 " + PrepareTrainingSet.class.getName(),
                "\n",
                options,
                "\n",
                true
        );
    }

    public Collection<FullyResolvedEntry> resolveDataset(Dataset dataset) {
        Collection<FullyResolvedEntry> result;
        if (index == null) {
            logger.info("Resolving dataset against Twitter");
            result = resolveDatasetViaTwitter(dataset);
        } else {
            logger.info("Resolving dataset against db index");
            result = resolveDatasetViaIndex(dataset);
        }

        //fillAdditionalData(result);

        return result;
    }

    public void initService() {
        if (service == null) {
            if (credentials == null) {
                logger.info("Twitter credentials are not set, skipping additional queries");
                return;
            }
            service = new TwitterService(SearchRunner.createInstances(credentials));
        }
    }

    public void fillAdditionalData(Collection<FullyResolvedEntry> dataset) {
        initService();

        //Query additional data
        List<UserData> candidates = dataset.stream().flatMap(entry -> entry.candidates.stream()).collect(Collectors.toList());
        fillAdditionalData(candidates, service);
    }

    public static void fillAdditionalData(List<UserData> candidates, TwitterService service) {
        //Query additional data
        long total = candidates.size();
        AtomicInteger processed = new AtomicInteger();
        StatusesProvider provider = new StatusesProvider(service);
        Function<UserData, Integer> resolveStatuses = (entry -> {
            boolean retry = true;
            while (retry) {
                try {
                    entry.populate(provider);
                    retry = false;
                } catch (TwitterService.RateLimitException limit) {
                    try {
                        logger.info(String.format("Limit reached, sleeping for %d seconds", limit.remaining));
                        Thread.sleep(limit.remaining * 1000);
                    } catch (InterruptedException e) {
                    }
                }
            }

            return processed.incrementAndGet();
        });

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ScheduledFuture<?> progressCheck = executor.scheduleWithFixedDelay(() -> {
            logger.info(String.format("[Twitter statuses] Progress: %5d/%5d", processed.get(), total));
        }, 1, 30, TimeUnit.SECONDS);

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        forkJoinPool.invokeAll(
            candidates
                .stream()
                .map(user -> (Callable<Integer>) () -> resolveStatuses.apply(user))
                .collect(Collectors.toList())
        );
        forkJoinPool.shutdown();
        progressCheck.cancel(true);
        executor.shutdownNow();
    }

    public List<FullyResolvedEntry> resolveDatasetViaIndex(Dataset dataset) {
        Objects.requireNonNull(dataset);
        Objects.requireNonNull(index);
        final AtomicInteger processed = new AtomicInteger(0);

        logger.info("Requesting entry info from knowledge base and index");
        //Checking the progress of the execution
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ScheduledFuture<?> progressCheck = executor.scheduleWithFixedDelay(() -> {
            logger.info(String.format("Processed %d entries", processed.get()));
        }, 1, 10, TimeUnit.SECONDS);

        //Resolving dataset entries
        List<FullyResolvedEntry> entries = new LinkedList<>();
        dataset.getEntries().parallelStream().forEach(datasetEntry -> {
            FullyResolvedEntry entry = new FullyResolvedEntry(datasetEntry);
            index.fill(entry);
            synchronized (entries) {
                entries.add(entry);
            }

            processed.incrementAndGet();
        });
        progressCheck.cancel(true);
        executor.shutdownNow();

        return entries;
    }

    public Collection<FullyResolvedEntry> resolveDatasetViaTwitter(Dataset dataset) {
        Objects.requireNonNull(dataset);
        Objects.requireNonNull(credentials);
        int processed = 0;

        //Selecting resources
        logger.info("Requesting entry info from knowledge base");
        Map<KBResource, FullyResolvedEntry> entries = new HashMap<>();
        for (DatasetEntry datasetEntry : dataset) {
            FullyResolvedEntry entry = new FullyResolvedEntry(datasetEntry);
            entry.resource = endpoint.getResourceById(datasetEntry.resourceId);
            entries.put(entry.resource, entry);

            if (++processed % 100 == 0) {
                logger.info(String.format("Processed %5d entries", processed));
            }
        }

        //Populating candidates
        logger.info("Populating candidates");
        if (credentials == null) {
            logger.error("This evaluation method requires TwitterCredentials");
            return new LinkedList<>();
        }

        SearchRunner.ResultReceiver receiver = new SearchRunner.ResultReceiver() {
            private int processed = 0;

            @Override
            public synchronized void processResult(List<User> candidates, KBResource task) {
                if (processed < 10) {
                    logger.info(String.format("Query: %s. Candidates: %d", qaStrategy.getQuery(task), candidates.size()));
                    processed++;
                }
                entries.get(task).candidates = candidates.stream().map(UserData::new).collect(Collectors.toList());
            }
        };
        SearchRunner[] runners;
        try {
            runners = SearchRunner.generateRunners(credentials, qaStrategy, receiver);
        } catch (TwitterException e) {
            logger.error("Can't instantiate runners", e);
            return new LinkedList<>();
        }

        SearchRunner.BatchProvider provider = new SearchRunner.BatchProvider() {
            @Override
            public List<KBResource> provideNextBatch() {
                List<KBResource> results = new LinkedList<>();
                for (FullyResolvedEntry entry : entries.values()) {
                    if (entry.candidates == null) {
                        results.add(entry.resource);
                    }
                    if (results.size() == SearchRunner.LIMIT * runners.length) {
                        return results;
                    }
                }
                return results;
            }

            @Override
            public void printStats(Date startDate, int processed) {
                logger.info("Done: " + processed);
            }
        };
        SearchRunner.startRun(runners, provider);

        return entries.values();
    }

    public void generateFeatures(List<FullyResolvedEntry> entries) {
        AtomicInteger processed = new AtomicInteger(0);
        Stopwatch watch = Stopwatch.createStarted();
        entries.parallelStream().forEach(entry -> {
            scoreStrategy.fillScore(entry);
            int curProc = processed.incrementAndGet();
            if (curProc % 10 == 0 && (watch.elapsed(TimeUnit.SECONDS) > 60 || curProc % 1000 == 0)) {
                synchronized (this) {
                    logger.info(String.format("Processed %d entities (%.2f seconds)", curProc, (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
                    watch.reset().start();
                }
            }
        });
    }

    public void purgeAdditionalData(List<FullyResolvedEntry> entries) {
        entries.parallelStream().flatMap(entry -> entry.candidates.stream()).forEach(UserData::clear);
    }

    public void dumpJointFeatures(List<FullyResolvedEntry> entries, FileProvider.FeatureSet output) throws IOException {
        Gson gson = new Gson();
        FileWriter jsonOutput = new FileWriter(output.JSONJointFeat);

        boolean first = true;
        for (FullyResolvedEntry entry : entries) {
            if (entry.candidates.size() == 0) {
                continue;
            }
            if (!first) {
                jsonOutput.write('\n');
            }
            first = false;

            int order = 0;
            JointSample curSample = new JointSample();
            curSample.samples = new PairSample[entry.candidates.size()];
            for (User user : entry.candidates) {
                int label = user.getScreenName().equalsIgnoreCase(entry.entry.twitterId) ? 1 : 0;
                Map<String, double[]> features = entry.features.get(order);
                curSample.samples[order] = new PairSample(label, features);
                if (label == 1) {
                    curSample.joint_label = label;
                }
                order++;
            }
            jsonOutput.write(gson.toJson(curSample));
        }
        IOUtils.closeQuietly(jsonOutput);
    }

    public void produceTrainTestSets(Dataset goldStandard, File trainSetFile, File testSetFile, double trainProb) throws IOException {
        FileWriter trainSetWriter = new FileWriter(trainSetFile);
        FileWriter testSetWriter = new FileWriter(testSetFile);
        trainSetWriter.write("entity,twitter_id\n");
        testSetWriter.write("entity,twitter_id\n");
        Random rnd = new Random();
        for (DatasetEntry entry : goldStandard) {
            String line = '"' + entry.resourceId + "\"," + entry.twitterId + '\n';
            if (rnd.nextDouble() <= trainProb) {
                trainSetWriter.write(line);
                continue;
            }
            testSetWriter.write(line);
        }
        IOUtils.closeQuietly(trainSetWriter);
        IOUtils.closeQuietly(testSetWriter);
    }

    /**
     * Predicts the id of the correct candidate
     *
     * @param positives list of candidates showing positive scores
     * @param maxImp    improvement to the second best that is needed
     * @param minScore  minimum score that is considered positive
     * @return id of the correct candidate or -1
     */
    private int getPrediction(List<double[]> positives, double maxImp, double minScore) {
        int predicted = -1;
        if (positives.size() > 0) {
            double maxScore = -1.0;
            double secondBestScore = -1.0;
            int maxId = 0;
            for (double[] positive : positives) {
                if (positive[1] > maxScore) {
                    secondBestScore = maxScore;
                    maxId = (int) positive[0];
                    maxScore = positive[1];
                    continue;
                }
                if (positive[1] > secondBestScore) {
                    secondBestScore = positive[1];
                }
            }
            if ((maxScore - secondBestScore) > maxImp && maxScore > minScore) {
                predicted = maxId;
            }
        }
        return predicted;
    }

    public String evaluate(Collection<FullyResolvedEntry> entries, ModelEndpoint endpoint) {
        return evaluate(entries, endpoint, false, new NullWriter());
    }

    public String evaluate(Collection<FullyResolvedEntry> entries, ModelEndpoint endpoint, boolean joint) {
        return evaluate(entries, endpoint, joint, new NullWriter());
    }

    public String evaluate(Collection<FullyResolvedEntry> entries, ModelEndpoint endpoint, boolean joint, Writer rawResult) {
        int gridImp = 5;
        int gridScore = 20;
        CustomEvaluation[] nnEvals = new CustomEvaluation[gridImp * gridScore];
        for (int i = 0; i < gridScore; i++) {
            for (int j = 0; j < gridImp; j++) {
                nnEvals[i * gridImp + j] = new CustomEvaluation().joint(joint);
            }
        }
        CustomEvaluation baselineStats = new CustomEvaluation().joint(joint);
        int sampleNum = 0;
        int entriesProc = 0;
        for (FullyResolvedEntry entry : entries) {
            entriesProc++;
            if (entriesProc % 1000 == 0) {
                logger.info(entriesProc + " candidates processed, " + (entries.size() - entriesProc) + " to go");
            }
            if (sampleNum <= 200) {
                logger.debug("Entry: " + entry.entry.resourceId);
                logger.debug("Query: " + new AllNamesStrategy().getQuery(entry.resource));
            }
            try {
                rawResult.write("Entry: " + entry.entry.resourceId + "\n");
                rawResult.write("Query: " + new AllNamesStrategy().getQuery(entry.resource) + "\n");
            } catch (Exception e) {
                logger.error("Raw result writer is broken");
            }
            if (entry.candidates.size() == 0) {
                if (joint) {
                    for (int i = 0; i < gridScore; i++) {
                        for (int j = 0; j < gridImp; j++) {
                            nnEvals[i * gridImp + j].getEval().addFN();
                        }
                    }
                    baselineStats.getEval().addFN();
                }
                continue;
            }
            int order = 0;
            List<double[]> modelPositives = new LinkedList<>();
            int trueLabel = -1;

            for (Map<String, double[]> features : entry.features) {
                double[] endpointScores = new double[2];

                if (endpoint != null) {
                    endpointScores = endpoint.predict(features);
                    modelPositives.add(new double[]{order, endpointScores[1]});
                }

                User candidate = entry.candidates.get(order);
                boolean isPositive = entry.entry.twitterId.equalsIgnoreCase(candidate.getScreenName());
                boolean isBaseline = order == 0;
                if (isPositive) {
                    trueLabel = order;
                }

                String report = String.format(
                        "%f\t%f\t%d\t%d\t\t%s\t\t\t%s",
                        endpointScores[0], endpointScores[1],
                        isPositive ? 1 : 0, isBaseline ? 1 : 0,
                        entry.entry.twitterId, candidate.getScreenName()
                );
                try {
                    rawResult.write(report);
                    rawResult.write('\n');
                } catch (Exception e) {
                    logger.error("Raw result writer is broken");
                }
                if (sampleNum <= 200) {
                    logger.debug(report);
                }
                order++;
                sampleNum++;
            }
            for (int i = 0; i < gridScore; i++) {
                for (int j = 0; j < gridImp; j++) {
                    nnEvals[i * gridImp + j].check(trueLabel, getPrediction(modelPositives, (double) j * 0.1, (double) i * 0.05));
                }
            }
            baselineStats.check(trueLabel, 0);
        }

        logger.info("Baseline:");
        baselineStats.printResult();
        StringBuilder result = new StringBuilder();
        if (endpoint != null) {
            logger.info("Neural Prec\tRec\tF1\tmaxImp\tminScore");
            for (int i = 0; i < gridScore; i++) {
                for (int j = 0; j < gridImp; j++) {
                    String hyper = String.format("\t%.1f\t%.2f\t", (double) j * 0.1, (double) i * 0.05);
                    String oneliner = nnEvals[i * gridImp + j].printOneliner();
                    if (j == 0) {
                        logger.info("       " + oneliner + hyper);
                    }
                    result.append("DNN\t");
                    result.append(oneliner);
                    result.append(hyper);
                    result.append('\n');
                }
            }
        }
        return result.toString();
    }

    public void resolveAndSaveDatasetChunk(Dataset dataset, int index, FileProvider files) {
        Stopwatch watch = Stopwatch.createStarted();
        logger.info("Resolving and dumping chunk with index " + index);
        Collection<FullyResolvedEntry> entries = resolveDataset(dataset);
        try {
            Writer resolvedWriter = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(new File(files.resolved, index + ".gz"))));
            GSON.toJson(entries, resolvedWriter);
            IOUtils.closeQuietly(resolvedWriter);
        } catch (IOException e) {
            logger.error("Something bad happened while saving chunk, skipping chunk " + index, e);
        }
        logger.info(String.format("Chunk %d completed in %.2f seconds", index, (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
    }

    public void setCredentials(TwitterCredentials[] credentials) {
        this.credentials = credentials;
    }

    public void setQAStrategy(QueryAssemblyStrategy qaStrategy) {
        if (qaStrategy == null) {
            return;
        }
        this.qaStrategy = qaStrategy;
    }

    public QueryAssemblyStrategy getQaStrategy() {
        return this.index == null ? qaStrategy : this.index.getQaStrategy();
    }

    public ScoringStrategy getScoreStrategy() {
        return scoreStrategy;
    }

    public void setScoreStrategy(ScoringStrategy scoreStrategy) {
        if (scoreStrategy == null) {
            return;
        }
        this.scoreStrategy = scoreStrategy;
    }

    public static class StatusesProvider implements UserData.DataProvider<List<JsonObject>, TwitterService.RateLimitException> {

        private TwitterService service;

        public StatusesProvider(TwitterService service) {
            this.service = service;
        }

        @Override
        public List<JsonObject> provide(User profile) throws TwitterService.RateLimitException {
            List<Status> statuses = service.getStatuses(profile.getId());
            return statuses
                    .stream()
                    .map(status -> GSON.fromJson(TwitterObjectFactory.getRawJSON(status), JsonObject.class))
                    .collect(Collectors.toList());
        }
    }

    public static class Configuration {
        String endpoint;

        ResourceEndpoint createEndpoint() throws Exception {
            if (endpoint.startsWith("http")) {
                return new Endpoint(endpoint);
            }

            File rdfPath =  new File(endpoint);
            if (!rdfPath.exists()) {
                throw new IOException("The endpoint at path \""+endpoint+"\" doesn't exist");
            }

            logger.info("The SPARQL endpoint looks like a file, loading into memory");
            return InMemoryEndpoint.uncompressAndLoad(rdfPath, new String[]{"en", "it", "de", "fr", "br", "en-ca", "en-gb", "ca", "pt"}, false);
        }

        String dbConnection;
        String dbUser;
        String dbPassword;
        String workdir;
        String credentials;
        String strategy;
        String lsa = null;
        String embeddings = null;
        String userDictionary = null;
        int modelPort = 5000;
    }

    public class PairSample {
        public final Map<String, double[]> features;
        public final int label;

        public PairSample(int label, Map<String, double[]> features) {
            this.label = label;
            this.features = features;
        }
    }

    public class JointSample {
        public int joint_label = 0;
        public PairSample[] samples;
    }
}
