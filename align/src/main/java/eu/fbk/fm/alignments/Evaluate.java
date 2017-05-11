package eu.fbk.fm.alignments;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.query.*;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.DefaultScoringStrategy;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import eu.fbk.fm.alignments.scorer.ISWC17Strategy;
import eu.fbk.fm.alignments.scorer.ScoringStrategy;
import eu.fbk.fm.alignments.twitter.SearchRunner;
import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.eval.PrecisionRecall;
import eu.fbk.utils.math.Scaler;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.TwitterException;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private ScoringStrategy scoreStrategy = new DefaultScoringStrategy();
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
        List<FullyResolvedEntry> entries = new LinkedList<>();
        dataset.getEntries().parallelStream().forEach(datasetEntry -> {
            FullyResolvedEntry entry = new FullyResolvedEntry(datasetEntry);
            index.fill(entry);
            entries.add(entry);

            int procValue = processed.incrementAndGet();
            if (procValue % 100 == 0) {
                logger.info("Processed " + procValue + " entries");
            }
        });

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
            if (curProc % 10 == 0 && (watch.elapsed(TimeUnit.SECONDS) > 60 || curProc % 1000 == 0)) {
                synchronized (this) {
                    logger.info(String.format("Processed %d entities (%.2f seconds)", curProc, (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
                    watch.reset().start();
                }
            }
        });
    }

    public void dumpContrastiveFeatures(List<FullyResolvedEntry> entries, FileProvider.FeatureSet output) throws IOException {
        CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(output.CSVContrastive), CSVFormat.DEFAULT);

        double[] zeros = null;
        Random rnd = new Random();
        for (FullyResolvedEntry entry : entries) {
            int candidates = entry.candidates.size();
            if (candidates == 0) {
                continue;
            }
            if (zeros == null) {
                zeros = new double[entry.features.get(0).length];
                Arrays.fill(zeros, 0.0);
            }

            int order = 0;
            boolean hasPositive = false;
            for (User user : entry.candidates) {
                if (user.getScreenName().equals(entry.entry.twitterId)) {
                    hasPositive = true;
                    break;
                }
                order++;
            }

            if (hasPositive) {
                double[] positiveFeatures = entry.features.get(order);

                for (int i = 0; i < candidates; i++) {
                    double value = 1.0;
                    if (order == i) {
                        value = 0.5;
                    }
                    csvPrinter.print(value);
                    for (double positiveFeature : positiveFeatures) {
                        csvPrinter.print(positiveFeature);
                    }
                    for (double feature : entry.features.get(i)) {
                        csvPrinter.print(feature);
                    }
                    csvPrinter.println();
                }
            }

            int featuresPrinted = 0;
            int iterations = 0;
            while (featuresPrinted < candidates && iterations < 1000) {
                int cand1 = rnd.nextInt(candidates);
                int cand2 = rnd.nextInt(candidates);
                if (cand1 != cand2 && (!hasPositive || (cand1 != order && cand2 != order))) {
                    csvPrinter.print(0.5);
                    for (double feature : entry.features.get(cand1)) {
                        csvPrinter.print(feature);
                    }
                    for (double feature : entry.features.get(cand2)) {
                        csvPrinter.print(feature);
                    }
                    csvPrinter.println();
                    featuresPrinted++;
                }
                iterations++;
            }
        }

        csvPrinter.close();
    }

    public void dumpFeatures(List<FullyResolvedEntry> entries, FileProvider.FeatureSet output) throws IOException {
        FileWriter svmOutput = new FileWriter(output.SVMFeat);
        CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(output.CSVFeat), CSVFormat.DEFAULT);
        CSVPrinter indexPrinter = new CSVPrinter(new FileWriter(output.index), CSVFormat.DEFAULT);

        for (FullyResolvedEntry entry : entries) {
            int order = 0;
            for (User user : entry.candidates) {
                indexPrinter.printRecord(entry.resource.getIdentifier(), user.getId(), user.getScreenName());
                boolean isPositive = user.getScreenName().equals(entry.entry.twitterId);
                double[] features = entry.features.get(order);

                csvPrinter.print(isPositive ? 1 : 0);
                svmOutput.write(String.valueOf(isPositive ? 1 : 0));
                for (int i = 0; i < features.length; i++) {
                    csvPrinter.print(features[i]);
                    svmOutput.write(" " + (i + 1) + ":");
                    svmOutput.write(String.valueOf(features[i]));
                }
                svmOutput.write('\n');
                csvPrinter.println();
                order++;
            }
        }
        IOUtils.closeQuietly(svmOutput);
        indexPrinter.close();
        csvPrinter.close();
    }

    private static void fitDataset(Scaler scaler, List<FullyResolvedEntry> entries) {
        List<double[]> features = new LinkedList<>();
        for (FullyResolvedEntry entry : entries) {
            features.addAll(entry.features);
        }
        scaler.fit(features);
    }

    private static void transformDataset(Scaler scaler, List<FullyResolvedEntry> entries) {
        List<double[]> features = new LinkedList<>();
        for (FullyResolvedEntry entry : entries) {
            scaler.transform(entry.features);
        }
    }

    public static class Dataset implements Iterable<DatasetEntry> {
        private String name = "default";
        private List<DatasetEntry> entries = new LinkedList<>();
        private Map<String, DatasetEntry> mappedEntries = new HashMap<>();

        protected Dataset() {
        }

        public List<DatasetEntry> getEntries() {
            return entries;
        }

        public int size() {
            return entries.size();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void add(DatasetEntry entry) {
            if (mappedEntries.containsKey(entry.resourceId)) {
                logger.error("This example is already in the dataset: " + entry.resourceId);
            }
            entries.add(entry);
            mappedEntries.put(entry.resourceId, entry);
        }

        public DatasetEntry findEntry(String resourceId) {
            return mappedEntries.get(resourceId);
        }

        @Override
        public Iterator<DatasetEntry> iterator() {
            return entries.iterator();
        }

        public static Dataset fromFile(File file) throws IOException {
            Dataset dataset = new Dataset();
            dataset.setName(file.getName());
            try (Reader reader = new FileReader(file)) {
                CSVParser parser = new CSVParser(
                        reader,
                        CSVFormat.DEFAULT.withDelimiter(',').withHeader()
                );
                for (CSVRecord record : parser) {
                    dataset.add(new DatasetEntry(record.get("entity"), record.get("twitter_id")));
                }
            }
            return dataset;
        }
    }

    public static class DatasetEntry {
        public String resourceId;
        public String twitterId;

        public DatasetEntry(String resourceId, String twitterId) {
            this.resourceId = resourceId;
            this.twitterId = twitterId;
        }

        public DatasetEntry(DBpediaResource resource) {
            this.resourceId = resource.getIdentifier();
            this.twitterId = null;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DatasetEntry)) {
                return super.equals(obj);
            }
            return resourceId.equals(((DatasetEntry) obj).resourceId);
        }

        @Override
        public int hashCode() {
            return resourceId.hashCode();
        }
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

    public static class CustomEvaluation {
        private boolean alwaysFNwhenNoAlign = false;
        private PrecisionRecall.Evaluator eval;

        public CustomEvaluation() {
            eval = PrecisionRecall.evaluator();
        }

        public CustomEvaluation joint() {
            return joint(true);
        }

        public CustomEvaluation joint(boolean joint) {
            alwaysFNwhenNoAlign = joint;
            return this;
        }

        public void check(int trueValue, int predicted) {
            if (trueValue == predicted) {
                //Prediction aligns
                if (predicted >= 0) {
                    //Prediction points to exact candidate
                    eval.addTP();
                } else if (alwaysFNwhenNoAlign) {
                    //Candidate not in the list (mistake in case of overall evaluation)
                    eval.addFN();
                }
            } else {
                //Prediction misaligns: mistake, there is a right candidate somewhere
                if (predicted >= 0) {
                    //Wrong prediction (not abstain). Counts as two errors
                    eval.addFP();
                    if (alwaysFNwhenNoAlign || trueValue != -1) {
                        eval.addFN();
                    }
                } else {
                    //Abstain. Always counts as false negative
                    eval.addFN();
                }
            }
        }

        public PrecisionRecall.Evaluator getEval() {
            return eval;
        }

        public void printResult() {
            PrecisionRecall result = eval.getResult();
            logger.info(String.format("Precision: %6.2f%%", result.getPrecision()*100));
            logger.info(String.format("Recall:    %6.2f%%", result.getRecall()*100));
            logger.info(String.format("F1:        %6.2f%%", result.getF1()*100));
        }

        public String printOneliner() {
            PrecisionRecall result = eval.getResult();
            return String.format("%.4f\t%.4f\t%.4f", result.getPrecision(), result.getRecall(), result.getF1());
        }
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
                logger.debug("Query: " + new NoQuotesDupesStrategy().getQuery(entry.resource));
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
            List<double[]> positives = new LinkedList<>();
            List<double[]> modelPositives = new LinkedList<>();
            int trueLabel = -1;

            for (double[] features : entry.features) {
                double label = 0, score = 0;
                double[] endpointScores = new double[2];

                if (endpoint != null) {
                    endpointScores = endpoint.predict(features);
                    //if (endpointScores[1] > 0.5) {
                    modelPositives.add(new double[]{order, endpointScores[1]});
                    //}
                }

                User candidate = entry.candidates.get(order);
                boolean isPositive = entry.entry.twitterId.toLowerCase().equals(candidate.getScreenName().toLowerCase());
                boolean isBaseline = order == 0;
                if (isPositive) {
                    trueLabel = order;
                }

                if (sampleNum <= 200) {
                    logger.debug(String.format("%f\t%.2f\t%.2f\t%.2f\t%d\t%d\t\t%s\t\t\t%s",
                            label, score, endpointScores[0], endpointScores[1], isPositive ? 1 : 0, isBaseline ? 1 : 0, entry.entry.twitterId, candidate.getScreenName())
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

    public static void evaluationPipeline(Evaluate evaluate, FileProvider files, List<FullyResolvedEntry> testSet) throws Exception {
        logger.info("Starting evaluation");
        if (testSet.size() == 0 || testSet.get(0).features.size() == 0) {
            logger.info("Test set is empty, skipping evaluation");
            return;
        }

        ModelEndpoint modelEndpoint = new ModelEndpoint();
        double[] prediction = modelEndpoint.predict(testSet.get(0).features.get(0));
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

    public static void main(String[] args) throws Exception {
        Gson gson = TwitterDeserializer.getDefault().getBuilder().create();

        Configuration configuration = loadConfiguration(args);
        if (configuration == null) {
            return;
        }

        QueryAssemblyStrategy qaStrategy = new AllNamesStrategy();//QueryAssemblyStrategyFactory.get(configuration.strategy);

        Endpoint endpoint = new Endpoint(configuration.endpoint);
        Evaluate evaluate;
        if (configuration.dbConnection != null && configuration.dbUser != null && configuration.dbPassword != null) {
            DataSource source = DBUtils.createPGDataSource(configuration.dbConnection, configuration.dbUser, configuration.dbPassword);
            evaluate = new Evaluate(new FillFromIndex(
                    endpoint,
                    qaStrategy,
                    source));
            if (configuration.lsa != null) {
                logger.info("LSA specified. Enabling ISWC17 strategy");
                evaluate.setScoreStrategy(new ISWC17Strategy(source, configuration.lsa));
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
            evaluationPipeline(evaluate, files, testSet);
            return;
        }

        try {
            Dataset goldStandardDataset = Dataset.fromFile(files.gold);
            if (!files.train.plain.exists() || !files.test.plain.exists()) {
                logger.info("Generating training/test sets");
                evaluate.produceTrainTestSets(goldStandardDataset, files.train.plain, files.test.plain, 0.8);
            }
            Dataset trainSetDataset = Dataset.fromFile(files.train.plain);
            Dataset testSetDataset = Dataset.fromFile(files.test.plain);

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
                    if (candidate.getScreenName().toLowerCase().equals(entry.entry.twitterId.toLowerCase())) {
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

            //Saving unscaled SVM and CSV features to disk
            evaluate.dumpFeatures(resolvedTrainingSet, files.train.unscaled);
            evaluate.dumpFeatures(resolvedTestSet, files.test.unscaled);

            Scaler scaler;
            if (files.scaler.exists()) {
                scaler = gson.fromJson(new FileReader(files.scaler), Scaler.class);
            } else {
                scaler = new Scaler();
                fitDataset(scaler, resolvedTrainingSet);
                //Saving scaler to disk
                FileUtils.writeStringToFile(files.scaler, gson.toJson(scaler));
            }

            //Rescaling features
            logger.info("Rescaling features");
            watch.reset().start();
            transformDataset(scaler, resolvedTrainingSet);
            transformDataset(scaler, resolvedTestSet);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));

            //Saving SVM and CSV features to disk
            evaluate.dumpFeatures(resolvedTrainingSet, files.train.scaled);
            evaluate.dumpFeatures(resolvedTestSet, files.test.scaled);
            //evaluate.dumpContrastiveFeatures(resolvedTrainingSet, files.train.scaled);

            logger.info("Dumping full experimental setting to JSON");
            watch.reset().start();
            FileWriter resolvedWriter = new FileWriter(files.evaluation);
            gson.toJson(resolvedTestSet.toArray(new FullyResolvedEntry[0]), resolvedWriter);
            IOUtils.closeQuietly(resolvedWriter);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
            evaluationPipeline(evaluate, files, resolvedTestSet);
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
        return qaStrategy;
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
