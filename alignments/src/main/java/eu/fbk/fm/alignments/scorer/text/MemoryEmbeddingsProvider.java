package eu.fbk.fm.alignments.scorer.text;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AtomicDouble;
import eu.fbk.utils.lsa.BOW;
import eu.fbk.utils.math.DenseVector;
import eu.fbk.utils.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Eats whatever embeddings are fed with optional idf rescaling from the LSA model
 *
 * Since the vocabularies can mismatch, idfs that were not found in LSA model
 *   are assigned to be the lowest observed frequency
 *   (priority is to not underestimate IDF for the unknown word)
 *
 * If LSA model is not provided the model doesn't include IDF into the computation
 */
public class MemoryEmbeddingsProvider implements VectorProvider {

    private static final Logger logger = LoggerFactory.getLogger(MemoryEmbeddingsProvider.class);
    private static final String[] SEPARATOR_SYMBOLS = new String[]{"\t", " "};

    private final Map<String, Integer> dictionary = new HashMap<>();
    private double[][] embeddings = null;
    private double[] idf = null;
    private String name;

    public MemoryEmbeddingsProvider(String embPath) throws IOException {
        init(embPath);
        idf = new double[this.embeddings.length];
        Arrays.fill(idf, 1.0d);
    }

    public MemoryEmbeddingsProvider(String embPath, String lsaPath) throws IOException {
        init(embPath);
        initIDF(lsaPath);
    }

    @Override
    public Vector toVector(String text) {
        BOW bow = new BOW(text);
        float[] vector = new float[embeddings[0].length];
        Arrays.fill(vector, 0.0f);

        // A very ugly procedure that basically does x * idf * embeddings, where x \in R^vsize, idf \in R^vsize
        for (String term : bow.termSet()) {
            if (!dictionary.containsKey(term)) {
                continue;
            }

            int idx = dictionary.get(term);
            for (int i = 0; i < vector.length; i++) {
                vector[i] += bow.tf(term) * idf[idx] * embeddings[idx][i];
            }
        }
        return new DenseVector(vector);
    }

    private void init(String embPath) throws IOException {
        logger.info("Reading embeddings from {}", embPath);
        name = Paths.get(embPath).getFileName().toString();
        Stopwatch stopwatch = Stopwatch.createStarted();
        LinkedList<double[]> embeddings = new LinkedList<>();

        final Splitter splitter = new Splitter();
        try(Stream<String> input = Files.lines(Paths.get(embPath))) {
            boolean parsed = input.allMatch((termLine) -> {
                String[] parsedLine = splitter.split(termLine);
                if (parsedLine.length <= 20) {
                    return false;
                }

                if (dictionary.containsKey(parsedLine[0])) {
                    logger.warn("  Duplicate entry for word {}", parsedLine[0]);
                    return true;
                }

                dictionary.put(parsedLine[0], dictionary.size());
                double[] vector = new double[parsedLine.length-1];
                for (int i = 0; i < parsedLine.length-1; i++) {
                    vector[i] = Double.valueOf(parsedLine[i+1]);
                }
                embeddings.add(vector);

                return true;
            });
            if (!parsed) {
                throw new IOException("Can't parse embeddings file");
            }
        }

        logger.info("  Loaded {} embeddings of size {}", embeddings.size(), splitter.getSize()-1);
        this.embeddings = new double[embeddings.size()][splitter.getSize()-1];
        this.embeddings = embeddings.toArray(this.embeddings);
        logger.info(String.format("  Done in %.2fs", ((double) stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000)));
    }

    private void initIDF(String lsaPath) {
        logger.info("Reading and computing IDFs from {}", lsaPath);
        Stopwatch stopwatch = Stopwatch.createStarted();

        idf = new double[this.embeddings.length];
        Arrays.fill(idf, 0.0d);

        AtomicInteger notfound = new AtomicInteger(0);
        AtomicInteger found = new AtomicInteger(0);
        AtomicDouble min = new AtomicDouble(Double.MAX_VALUE);
        try(Stream<String> input = Files.lines(Paths.get(lsaPath, "X-df"))) {
            long documentCount = Files.lines(Paths.get(lsaPath, "X-col")).count();
            boolean parsed = input.allMatch((termLine) -> {
                String[] parsedLine = termLine.split("\t");
                if (parsedLine.length != 2) {
                    return false;
                }

                if (!dictionary.containsKey(parsedLine[1])) {
                    notfound.incrementAndGet();
                    return true;
                }
                found.incrementAndGet();

                double termFrequency = Double.valueOf(parsedLine[0]);
                if (min.get() > termFrequency) {
                    min.set(termFrequency);
                }
                idf[dictionary.get(parsedLine[1])] = idf(documentCount, termFrequency);

                return true;
            });
            if (!parsed) {
                throw new IOException();
            }

            for (int i = 0; i < idf.length; i++) {
                if (idf[i] == 0.0d) {
                    idf[i] = idf(documentCount, min.get());
                }
            }
        } catch (IOException e) {
            logger.error("Can't parse LSA DF file: ", e);
            Arrays.fill(idf, 1.0d);
            return;
        }

        logger.info(String.format("  Matched tokens: %d (%.2f%%). Wasted tokens: %d (%.2f%%). Minimum term frequency: %.2f",
            found.get(), ((double) found.get() / idf.length) * 100,
            notfound.get(), ((double) notfound.get() / (found.get() + notfound.get())) * 100,
            min.get()
        ));
        logger.info(String.format("  Done in %.2fs", ((double) stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000)));
    }

    private double idf(double documentCount, double termFrequency) {
        return Math.log10(1 + documentCount / (1 + termFrequency));
    }

    private static class Splitter {
        private String symbol = null;
        private int size = 0;

        public String[] split(String line) {
            if (symbol != null) {
                return line.split(symbol);
            }

            String[] parsedLine = new String[0];
            for (String symbol : SEPARATOR_SYMBOLS) {
                parsedLine = line.split(symbol);
                if (parsedLine.length > 20) {
                    this.symbol = symbol;
                    this.size = parsedLine.length;
                    break;
                }
            }
            return parsedLine;
        }

        public Optional<String> getSymbol() {
            return Optional.ofNullable(symbol);
        }

        public int getSize() {
            return size;
        }
    }

    @Override
    public String toString() {
        return "emb_"+this.name;
    }
}
