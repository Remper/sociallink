package eu.fbk.fm.alignments;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import eu.fbk.fm.alignments.evaluation.*;
import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.query.*;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.*;
import eu.fbk.fm.alignments.twitter.SearchRunner;
import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.math.Scaler;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Script that evaluates a particular alignments pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Evaluate {

    private static final Logger logger = LoggerFactory.getLogger(Evaluate.class);
    public static final int CANDIDATES_THRESHOLD = 40;

    private Endpoint endpoint;
    private TwitterCredentials[] credentials = null;
    private QueryAssemblyStrategy qaStrategy = QueryAssemblyStrategyFactory.def();
    private ScoringStrategy scoreStrategy = null;
    private Gson gson;

    private FillFromIndex index = null;

    public Evaluate(Endpoint endpoint) throws Exception {
        Objects.requireNonNull(endpoint);
        this.endpoint = endpoint;
        init();
    }

    public Evaluate(FillFromIndex index) throws Exception {
        Objects.requireNonNull(index);
        this.index = index;
        init();
    }

    public void init() {
        this.gson = TwitterDeserializer.getDefault().getBuilder().create();
    }

    public Collection<FullyResolvedEntry> resolveDataset(Dataset dataset) {
        if (index == null) {
            logger.info("Resolving dataset against Twitter");
            return resolveDatasetViaTwitter(dataset);
        }

        logger.info("Resolving dataset against db index");
        return resolveDatasetViaIndex(dataset);
    }

    public Collection<FullyResolvedEntry> resolveDatasetViaIndex(Dataset dataset) {
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
        Map<DBpediaResource, FullyResolvedEntry> entries = new HashMap<>();
        for (DatasetEntry datasetEntry : dataset) {
            FullyResolvedEntry entry = new FullyResolvedEntry(datasetEntry);
            entry.resource = endpoint.getResourceById(datasetEntry.resourceId);
            entries.put(entry.resource, entry);

            if (++processed % 100 == 0) {
                logger.info("Processed " + processed + " entries");
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
            public synchronized void processResult(List<User> candidates, DBpediaResource task) {
                if (processed < 10) {
                    logger.info("Query: " + qaStrategy.getQuery(task) + ". Candidates: " + candidates.size());
                    processed++;
                }
                entries.get(task).candidates = candidates;
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
            public List<DBpediaResource> provideNextBatch() {
                List<DBpediaResource> results = new LinkedList<>();
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
            if (curProc % 10 == 0 && (watch.elapsed(TimeUnit.SECONDS) > 120 || curProc % 1000 == 0)) {
                synchronized (this) {
                    logger.info(String.format("Processed %d entities (%.2f seconds)", curProc, (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
                    watch.reset().start();
                }
            }
        });
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

    public void dumpJointFeatures(List<FullyResolvedEntry> entries, FileProvider.FeatureSet output) throws IOException {
        Gson gson = new Gson();
        FileWriter jsonOutput = new FileWriter(output.JSONJointFeat);

        boolean first = true;
        for (FullyResolvedEntry entry : entries) {
            int order = 0;
            JointSample curSample = new JointSample();
            curSample.samples = new PairSample[entry.candidates.size()];
            for (User user : entry.candidates) {
                if (!first) {
                    jsonOutput.write('\n');
                }
                first = false;

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

    public void dumpFeatures(List<FullyResolvedEntry> entries, FileProvider.FeatureSet output) throws IOException {
        Gson gson = new Gson();
        FileWriter jsonOutput = new FileWriter(output.JSONFeat);
        CSVPrinter indexPrinter = new CSVPrinter(new FileWriter(output.index), CSVFormat.DEFAULT);

        boolean first = true;
        for (FullyResolvedEntry entry : entries) {
            int order = 0;
            for (User user : entry.candidates) {
                if (!first) {
                    jsonOutput.write('\n');
                }
                first = false;

                indexPrinter.printRecord(entry.resource.getIdentifier(), entry.entry.twitterId, user.getId(), user.getScreenName());
                boolean isPositive = user.getScreenName().equalsIgnoreCase(entry.entry.twitterId);
                Map<String, double[]> features = entry.features.get(order);

                jsonOutput.write(String.valueOf(isPositive ? 1 : 0));
                jsonOutput.write('\t');
                jsonOutput.write(gson.toJson(features));
                order++;
            }
        }
        IOUtils.closeQuietly(jsonOutput);
        indexPrinter.close();
    }

    private static Map<String, Scaler> fitDataset(List<FullyResolvedEntry> entries) {
        HashMap<String, List<double[]>> features = new HashMap<>();
        HashMap<String, Scaler> scalers = new HashMap<>();

        entries.stream()
                //For each feature vector in a dataset entry
                .flatMap(entry -> entry.features.stream())
                //For each feature subspace
                .flatMap(map -> map.entrySet().stream())
                //Create scaler if needed and add feature subspace in an appropriate bucket
                .forEach(entry -> {
                    if (!features.containsKey(entry.getKey())) {
                        features.put(entry.getKey(), new LinkedList<>());
                        scalers.put(entry.getKey(), new Scaler());
                    }
                    features.get(entry.getKey()).add(entry.getValue());
                });

        scalers.entrySet().parallelStream()
                //Fit scalers for each bucket (feature subspace)
                .forEach(entry -> entry.getValue().fit(features.get(entry.getKey())));

        return scalers;
    }

    private static void transformDataset(Map<String, Scaler> scalers, List<FullyResolvedEntry> entries) {
        //Transform everything by default
        transformDataset(scalers, entries, null);
    }

    private static void transformDataset(Map<String, Scaler> scalers, List<FullyResolvedEntry> entries, String[] whitelist) {
        entries.parallelStream()
                //For each feature vector in a dataset entry
                .flatMap(entry -> entry.features.stream())
                //For each feature subspace
                .flatMap(map -> map.entrySet().stream())
                //Check if we should transform this subspace
                .filter(entry -> whitelist == null || Stream.of(whitelist).anyMatch(ele -> ele.equals(entry.getKey())))
                //Transform the subspace
                .forEach(entry -> scalers.get(entry.getKey()).transform(entry.getValue()));
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

    private int getPrediction(List<double[]> positives, double maxImp, double minScore) {
        int predicted = -1;
        if (positives.size() > 0) {
            double maxScore = -200.0;
            int maxId = 0;
            double lastImprovement = 0.0;
            for (double[] positive : positives) {
                if (positive[1] > maxScore) {
                    maxId = (int) positive[0];
                    lastImprovement = positive[1] - maxScore;
                    maxScore = positive[1];
                }
            }
            if (lastImprovement > maxImp && maxScore > minScore) {
                predicted = maxId;
            }
        }
        return predicted;
    }

    public String evaluate(Collection<FullyResolvedEntry> entries, ModelEndpoint endpoint) {
        return evaluate(entries, endpoint, false);
    }

    public String evaluate(Collection<FullyResolvedEntry> entries, ModelEndpoint endpoint, boolean joint) {
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
                double label = 0, score = 0;
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

                if (sampleNum <= 200) {
                    logger.debug(String.format(
                        "%f\t%.2f\t%.2f\t%.2f\t%d\t%d\t\t%s\t\t\t%s",
                        label, score,
                        endpointScores[0], endpointScores[1],
                        isPositive ? 1 : 0, isBaseline ? 1 : 0,
                        entry.entry.twitterId, candidate.getScreenName())
                    );
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

    public static void evaluationPipeline(Evaluate evaluate, FileProvider files, List<FullyResolvedEntry> testSet, ModelEndpoint modelEndpoint) throws Exception {
        logger.info("Starting evaluation");
        if (testSet.size() == 0) {
            logger.info("Test set is empty, skipping evaluation");
            return;
        }

        int i = 0;
        while (testSet.get(i).features.size() == 0) {
            i++;
        }
        if (i == testSet.size()) {
            logger.warn("Set was nonempty but all the elements lack candidates");
            return;
        }
        double[] prediction = modelEndpoint.predict(testSet.get(i).features.get(0));
        if (prediction.length == 0) {
            logger.info("The neural model hasn't been trained. Skipping evaluation");
            return;
        }

        List<FullyResolvedEntry> testSetPersons = new LinkedList<>();
        List<FullyResolvedEntry> testSetOrganisations = new LinkedList<>();
        for (FullyResolvedEntry entry : testSet) {
            if (entry.resource.isPerson()) {
                testSetPersons.add(entry);
            }
            if (entry.resource.isCompany()) {
                testSetOrganisations.add(entry);
            }
        }
        logger.info("Persons in a test set: " + testSetPersons.size());
        logger.info("Orgs    in a test set: " + testSetOrganisations.size());

        FileWriter writer = new FileWriter(files.evaluationResult);
        for (boolean joint : new boolean[]{false, true}) {
            logger.info(joint ? "Joint eval:" : "Selection eval:");
            writer.write(joint ? "Joint eval:" : "Selection eval:");
            writer.write('\n');
            writer.write(evaluate.evaluate(testSet, modelEndpoint, joint));
            Thread.sleep(60000);
            logger.info("Persons:");
            writer.write("Persons\n");
            writer.write(evaluate.evaluate(testSetPersons, modelEndpoint, joint));
            Thread.sleep(60000);
            logger.info("Organisations:");
            writer.write("Organisations\n");
            writer.write(evaluate.evaluate(testSetOrganisations, modelEndpoint, joint));
            Thread.sleep(60000);
        }
        writer.close();
    }

    private static void strategiesCheck(Collection<FullyResolvedEntry> resolveDataset) {
        //Here we check all the strategies doing a random pick from the resolved dataset
        logger.info("Strategies check");
        QueryAssemblyStrategy[] strategies = {
                new AllNamesStrategy(),
                new NoQuotesDupesStrategy(),
                new StrictQuotesStrategy(),
                new StrictStrategy(),
                new StrictWithTopicStrategy()
        };
        Iterator<FullyResolvedEntry> datasetIterator = resolveDataset.iterator();
        Random rnd = new Random();
        for (int i = 0; i < 10; i++) {
            FullyResolvedEntry entry;
            do {
                entry = datasetIterator.next();

                if (!datasetIterator.hasNext()) {
                    datasetIterator = resolveDataset.iterator();
                }
            } while (rnd.nextDouble() > 0.1);

            logger.info("Entity: " + entry.entry.resourceId + ". True alignments: @" + entry.entry.twitterId);
            logger.info("  Names: " + String.join(", ", entry.resource.getNames()));
            for (QueryAssemblyStrategy strategy : strategies) {
                logger.info("  Strategy: " + strategy.getClass().getSimpleName() + ". Query: " + strategy.getQuery(entry.resource));
            }
            if (entry.candidates == null || entry.candidates.size() == 0) {
                logger.info("  No candidates");
            } else {
                logger.info("  Candidates:");
                for (User candidate : entry.candidates) {
                    logger.info("    @" + candidate.getScreenName() + " (" + candidate.getName() + ")");
                }
            }
        }
        logger.info("Strategies check finished");
    }

    public static void main(String[] args) throws Exception {
        Gson gson = TwitterDeserializer.getDefault().getBuilder().create();

        Configuration configuration = loadConfiguration(args);
        if (configuration == null) {
            return;
        }

        logger.info(String.format("Options %s", gson.toJson(configuration)));

        QueryAssemblyStrategy qaStrategy = new AllNamesStrategy();//QueryAssemblyStrategyFactory.get(configuration.strategy);

        Endpoint endpoint = new Endpoint(configuration.endpoint);
        Evaluate evaluate;
        if (configuration.dbConnection != null && configuration.dbUser != null && configuration.dbPassword != null) {
            DataSource source = DBUtils.createHikariDataSource(configuration.dbConnection, configuration.dbUser, configuration.dbPassword);
            evaluate = new Evaluate(new FillFromIndex(
                    endpoint,
                    qaStrategy,
                    source));
            if (configuration.lsa != null) {
                logger.info("LSA specified. Enabling PAI18 strategy");
                evaluate.setScoreStrategy(new PAI18Strategy(source, configuration.lsa));
            } else {
                logger.info("LSA is not specified. Falling back to the default strategy");
            }
        } else {
            evaluate = new Evaluate(endpoint);
            evaluate.setQAStrategy(qaStrategy);
        }

        FileProvider files = new FileProvider(configuration.workdir);
        if (!files.gold.exists()) {
            logger.error("Gold standard dataset doesn't exist");
            return;
        }

        if (files.evaluation.exists()) {
            List<FullyResolvedEntry> testSet = new LinkedList<>();
            FullyResolvedEntry[] testSetRaw = gson.fromJson(new FileReader(files.evaluation), FullyResolvedEntry[].class);
            logger.info("Deserialised " + testSetRaw.length + " entities");
            Collections.addAll(testSet, testSetRaw);
            evaluationPipeline(evaluate, files, testSet, new ModelEndpoint("localhost", configuration.modelPort));
            return;
        }

        //Loading full gold standard
        Dataset goldStandardDataset = Dataset.fromFile(files.gold);

        //Resolving all the data needed for analysis
        Collection<FullyResolvedEntry> resolveDataset;
        if (!files.resolved.exists()) {
            resolveDataset = new LinkedList<>();

            if (configuration.credentials != null) {
                evaluate.setCredentials(TwitterCredentials.credentialsFromFile(new File(configuration.credentials)));
            }
            logger.info("Resolving all data from the dataset (" + goldStandardDataset.size() + " entries)");
            logger.info("Query strategy: " + evaluate.getQaStrategy().getClass().getSimpleName());
            resolveDataset.addAll(evaluate.resolveDataset(goldStandardDataset));
            FileWriter resolvedWriter = new FileWriter(files.resolved);
            gson.toJson(resolveDataset.toArray(new FullyResolvedEntry[0]), resolvedWriter);
            IOUtils.closeQuietly(resolvedWriter);
        } else {
            logger.info("Deserialising user data");
            Stopwatch watch = Stopwatch.createStarted();
            resolveDataset = new LinkedList<>();
            Collections.addAll(resolveDataset, gson.fromJson(new FileReader(files.resolved), FullyResolvedEntry[].class));
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
        }

        strategiesCheck(resolveDataset);

        //Produce multiple train/test splits
        if (!files.train.plain.exists() || !files.test.plain.exists()) {
            int numSplits = 2;
            FileProvider provider = null;
            for (int i = numSplits; i > 0; i--) {
                File newWorkdir = new File(configuration.workdir, "split"+i);
                if (!newWorkdir.mkdir()) {
                    logger.error("Error while creating subdirectory for split "+i);
                }
                provider = new FileProvider(newWorkdir);
                logger.info("Generating training/test sets for split"+i);
                evaluate.produceTrainTestSets(goldStandardDataset, provider.train.plain, provider.test.plain, 0.8);
                Files.createSymbolicLink(provider.resolved.toPath(), files.resolved.toPath());
                Files.copy(files.gold.toPath(), provider.gold.toPath());
            }
            //Picking one of the providers to continue
            files = provider;
        }

        try {
            Dataset trainSetDataset = Dataset.fromFile(files.train.plain);
            Dataset testSetDataset = Dataset.fromFile(files.test.plain);

            List<FullyResolvedEntry> resolvedTestSet = new LinkedList<>();
            List<FullyResolvedEntry> resolvedTrainingSet = new LinkedList<>();
            Set<String> resourceIds = new HashSet<>();
            int numCandidates = 0;
            int numNoCandidates = 0;
            int trueCandidates = 0;
            int[] trueCandidatesOrder = new int[CANDIDATES_THRESHOLD];
            for (FullyResolvedEntry entry : resolveDataset) {
                if (entry == null) {
                    logger.error("Entry is null for some reason!");
                    continue;
                }
                numCandidates += entry.candidates.size();
                if (entry.candidates.size() == 0) {
                    numNoCandidates++;
                }
                int order = 0;
                for (User candidate : entry.candidates) {
                    if (candidate.getScreenName().equalsIgnoreCase(entry.entry.twitterId)) {
                        trueCandidates++;
                        if (order < CANDIDATES_THRESHOLD) {
                            trueCandidatesOrder[order]++;
                        }
                        break;
                    }
                    order++;
                }
                if (resourceIds.contains(entry.entry.resourceId)) {
                    continue;
                }
                resourceIds.add(entry.entry.resourceId);

                if (testSetDataset.findEntry(entry.entry.resourceId) != null) {
                    resolvedTestSet.add(entry);
                    continue;
                }
                if (trainSetDataset.findEntry(entry.entry.resourceId) != null) {
                    resolvedTrainingSet.add(entry);
                }
            }
            if (resolvedTestSet.size() + resolvedTrainingSet.size() != resolveDataset.size()) {
                logger.warn("For some reason we lost some data while preparing datasets");
            }
            logger.info("Dataset statistics: ");
            logger.info(" Items before resolving:\t" + goldStandardDataset.size() + "\t(" + trainSetDataset.size() + "+" + testSetDataset.size() + ")");
            logger.info(" Items after resolving:\t" + resolveDataset.size() + "\t(" + resolvedTrainingSet.size() + "+" + resolvedTestSet.size() + ")");
            int lost = goldStandardDataset.size() - resolveDataset.size();
            logger.info(String.format(" Lost:\t%d (%.2f", lost, ((double) lost / goldStandardDataset.size()) * 100) + "%)");
            logger.info(String.format(" Average candidates per entity: %.2f", (double) numCandidates / resolveDataset.size()));
            logger.info(String.format(" Entities without candidates: %d (%.2f", numNoCandidates, ((double) numNoCandidates / resolveDataset.size()) * 100) + "%)");
            logger.info(String.format(" Entities with true candidate: %d (%.2f", trueCandidates, ((double) trueCandidates / resolveDataset.size()) * 100) + "%)");
            logger.info(" True candidates distribution: ");
            int candSum = 0;
            for (int i = 0; i < trueCandidatesOrder.length; i++) {
                candSum += trueCandidatesOrder[i];
                logger.info(String.format("  %d — %d (%.2f", i, candSum, ((double) candSum / resolveDataset.size()) * 100) + "%)");
            }

            //Generating features
            logger.info("Generating features");

            Stopwatch watch = Stopwatch.createStarted();
            evaluate.generateFeatures(resolvedTrainingSet);
            evaluate.generateFeatures(resolvedTestSet);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));

            //Saving unscaled JSON features to disk
            //evaluate.dumpFeatures(resolvedTrainingSet, files.train.unscaled);
            //evaluate.dumpFeatures(resolvedTestSet, files.test.unscaled);

            Map<String, Scaler> scalers;
            if (files.scaler.exists()) {
                scalers = gson.fromJson(new FileReader(files.scaler), files.scalerType);
            } else {
                scalers = fitDataset(resolvedTrainingSet);
                //Saving scaler to disk
                FileUtils.writeStringToFile(files.scaler, gson.toJson(scalers));
            }

            //Rescaling features
            logger.info("Rescaling features");
            watch.reset().start();
            transformDataset(scalers, resolvedTrainingSet);
            transformDataset(scalers, resolvedTestSet);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));

            //Saving JSON features to disk
            logger.info("Dumping features");
            watch.reset().start();
            evaluate.dumpFeatures(resolvedTrainingSet, files.train.scaled);
            evaluate.dumpFeatures(resolvedTestSet, files.test.scaled);
            evaluate.dumpJointFeatures(resolvedTrainingSet, files.test.scaled);
            evaluate.dumpJointFeatures(resolvedTestSet, files.test.scaled);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));

            logger.info("Dumping full experimental setting to JSON");
            watch.reset().start();
            FileWriter resolvedWriter = new FileWriter(files.evaluation);
            gson.toJson(resolvedTestSet.toArray(new FullyResolvedEntry[0]), resolvedWriter);
            IOUtils.closeQuietly(resolvedWriter);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
            evaluationPipeline(evaluate, files, resolvedTestSet, new ModelEndpoint("localhost", configuration.modelPort));
        } catch (Exception e) {
            logger.error("Error while processing pipeline", e);
            e.printStackTrace();
        }
    }

    public static class Configuration {
        String endpoint;
        String dbConnection;
        String dbUser;
        String dbPassword;
        String workdir;
        String credentials;
        String strategy;
        String lsa = null;
        int modelPort = 5000;
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("e").desc("Url to SPARQL endpoint")
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
                "java -Dfile.encoding=UTF-8 eu.fbk.ict.fm.profiling.converters.eu.fbk.fm.alignments.Evaluate",
                "\n",
                options,
                "\n",
                true
        );
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
}
