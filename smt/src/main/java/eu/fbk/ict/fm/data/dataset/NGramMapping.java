package eu.fbk.ict.fm.data.dataset;

import eu.fbk.ict.fm.data.DatasetMetaInfo;
import eu.fbk.ict.fm.data.util.Stopwatch;

import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * A mapping between an ngram and it's corresponding index (thewikimachine version)
 * The format is
 * ngram index weight
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class NGramMapping extends FeatureMapping {
    public NGramMapping(DatasetMetaInfo info) throws URISyntaxException {
        super(info);
    }

    @Override
    public void parse() {
        //Parse the input file
        features = new HashMap<>();
        int index = 0;
        try (LineNumberReader reader = getReader()) {
            String line;
            Stopwatch watch = Stopwatch.start();
            double maxValue = 0.0d;
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split("\t");
                //Sanitizing the the word
                Feature feature = new Feature();
                feature.index = index;
                String ngram = elements[2].replace(' ', '_').toLowerCase();
                feature.weight = 1.0d / Double.valueOf(elements[0]);
                if (Double.isNaN(feature.weight)) {
                    feature.weight = 0.0d;
                }
                if (feature.weight > maxValue) {
                    maxValue = feature.weight;
                }
                features.put(ngram, feature);
                index++;
                if (index % 1000000 == 0) {
                    logger.info(String.format("Parsed %2dm ngrams (%.2f seconds)",
                            index / 1000000,
                            (double) watch.click() / 1000
                    ));
                }
            }
            logger.info("Rescaling weights...");
            for (Feature feature : features.values()) {
                feature.weight = Math.log(1 + maxValue * feature.weight);
            }
            logger.info("Parsing finished with " + features.size() + " ngrams");
        } catch (IOException e) {
            logger.error("Can't parse the input file: " + e.getClass().getSimpleName() + " " + e.getMessage());
        }
    }

    public Map<String, Feature> getRawMap() {
        return features;
    }
}
