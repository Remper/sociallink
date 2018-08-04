package eu.fbk.fm.alignments;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.fbk.fm.alignments.evaluation.Dataset;
import eu.fbk.fm.alignments.evaluation.DatasetEntry;
import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.*;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.MemoryEmbeddingsProvider;
import eu.fbk.fm.alignments.scorer.text.VectorProvider;
import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.fm.alignments.twitter.TwitterDeserializer;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.lsa.LSM;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static eu.fbk.fm.alignments.PrepareTrainingSet.RESOLVE_CHUNK_SIZE;
import static eu.fbk.fm.alignments.scorer.TextScorer.DBPEDIA_TEXT_EXTRACTOR;

/**
 * A sibling to PrepareTrainingSet but tailored towards inference instead of full training+evaluation pipeline
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ProcessDataset {
    private static final Logger logger = LoggerFactory.getLogger(ProcessDataset.class);

    public static void main(String[] args) throws Exception {
        Gson gson = TwitterDeserializer.getDefault().getBuilder().create();

        PrepareTrainingSet.Configuration configuration = PrepareTrainingSet.loadConfiguration(args);
        if (configuration == null) {
            return;
        }

        if (configuration.dbConnection == null || configuration.dbUser == null || configuration.dbPassword == null) {
            logger.error("DB credentials are not specified");
            return;
        }

        logger.info(String.format("Options %s", gson.toJson(configuration)));

        QueryAssemblyStrategy qaStrategy = new AllNamesStrategy();

        Endpoint endpoint = new Endpoint(configuration.endpoint);
        PrepareTrainingSet prepareTrainingSet;

        DataSource source = DBUtils.createHikariDataSource(configuration.dbConnection, configuration.dbUser, configuration.dbPassword);
        prepareTrainingSet = new PrepareTrainingSet(new FillFromIndex(endpoint, qaStrategy, source));

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
        strategy.addProvider(ISWC17Strategy.builder().vectorProviders(allVectorProviders).build());
        prepareTrainingSet.setScoreStrategy(strategy);
        //prepareTrainingSet.setScoreStrategy(new PAI18Strategy(new LinkedList<>()));
        //prepareTrainingSet.setScoreStrategy(new SMTStrategy(source, configuration.lsa));

        FileProvider files = new FileProvider(configuration.workdir);
        if (!files.input.exists()) {
            logger.error("Input dataset doesn't exist");
            return;
        }

        //Loading full input dataset
        Dataset inputDataset = Dataset.fromFile(files.input);

        if (configuration.credentials != null) {
            prepareTrainingSet.setCredentials(TwitterCredentials.credentialsFromFile(new File(configuration.credentials)));
        }

        //Resolving all the data needed for analysis
        int curLastIndex = -1;
        int totalChunks = (int) Math.ceil((float) inputDataset.size() / RESOLVE_CHUNK_SIZE);
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
                inputDataset.size(), toSkip, totalChunks
            ));
            logger.info("Query strategy: " + prepareTrainingSet.getQaStrategy().getClass().getSimpleName());

            Dataset curDataset = new Dataset();
            for (DatasetEntry entry : inputDataset) {
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
        AtomicInteger dead = new AtomicInteger(0);
        AtomicInteger empty = new AtomicInteger(0);
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
            List<FullyResolvedEntry> filteredChunk = entries
                .stream()
                .filter(entry -> !entry.resource.isDead()).collect(Collectors.toList());
            dead.addAndGet(entries.size()-filteredChunk.size());
            prepareTrainingSet.generateFeatures(entries);
            prepareTrainingSet.purgeAdditionalData(entries);
            filteredChunk = filteredChunk
                .stream()
                .filter(entry -> entry.candidates.size() > 0)
                .collect(Collectors.toList());
            empty.addAndGet(entries.size()-filteredChunk.size());

            synchronized (resolveDataset) {
                resolveDataset.addAll(filteredChunk);
            }

            IOUtils.closeQuietly(reader);
            logger.info(String.format(
                "Chunk %s completed in %.2f seconds (left %2d)",
                file.getName(),
                (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000,
                chunksLeft.decrementAndGet()
            ));
        });

        try {
            int numCandidates = resolveDataset
                    .stream()
                    .map(entry -> entry.candidates.size())
                    .reduce(0, (i1, i2) -> i1+i2);

            logger.info("Dataset statistics: ");
            logger.info(" Items before resolving:\t" + inputDataset.size());
            logger.info(" Items after resolving:\t" + resolveDataset.size());
            int lost = inputDataset.size() - resolveDataset.size();
            logger.info(String.format(
                " Lost:\t%d (%.2f%%, dead: %d, empty: %d)",
                lost, ((double) lost / inputDataset.size()) * 100,
                dead.get(), empty.get()-dead.get()
            ));
            logger.info(String.format(" Average candidates per entity: %.2f", (double) numCandidates / resolveDataset.size()));

            Stopwatch watch = Stopwatch.createStarted();
            logger.info("Dumping full experimental setting to JSON");
            FileWriter resolvedWriter = new FileWriter(files.dataset);
            boolean first = true;
            for (FullyResolvedEntry entry : resolveDataset) {
                if (first) {
                    first = false;
                } else {
                    resolvedWriter.write('\n');
                }
                gson.toJson(entry, resolvedWriter);
            }
            IOUtils.closeQuietly(resolvedWriter);
            logger.info(String.format("Complete in %.2f seconds", (double) watch.elapsed(TimeUnit.MILLISECONDS) / 1000));
        } catch (Exception e) {
            logger.error("Error while processing pipeline", e);
            e.printStackTrace();
        }
    }
}
