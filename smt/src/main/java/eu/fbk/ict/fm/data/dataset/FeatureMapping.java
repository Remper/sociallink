package eu.fbk.ict.fm.data.dataset;

import eu.fbk.ict.fm.data.DatasetMetaInfo;
import eu.fbk.ict.fm.data.ngrams.FeatureMappingInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A mapping between an ngram and it's corresponding index
 * The format is
 * ngram index weight
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FeatureMapping extends Dataset implements FeatureMappingInterface {
    final static Logger logger = LoggerFactory.getLogger(FeatureMapping.class);

    protected Map<String, Feature> features;

    public FeatureMapping(DatasetMetaInfo info) throws URISyntaxException {
        super(info);
    }

    public static class Feature {
        public int index;
        public double weight;

        @Override
        public String toString() {
            return index + ";" + weight;
        }

        public static Feature fromString(String string) {
            if (string == null) {
                return null;
            }
            Feature feature = new Feature();
            String[] stringArr = string.split(";");
            feature.index = Integer.valueOf(stringArr[0]);
            feature.weight = Double.valueOf(stringArr[1]);
            return feature;
        }
    }

    @Override
    public void parse() {
        //Parse the input file
        features = new HashMap<>();
        try (LineNumberReader reader = getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] elements = line.split("\\s+");
                //Sanitizing the the word
                Feature feature = new Feature();
                feature.index = Integer.valueOf(elements[0]);
                feature.weight = Double.valueOf(elements[2]);
                features.put(elements[1], feature);
            }

            logger.info("Parsed " + features.size() + " ngrams");
        } catch (IOException e) {
            logger.error("Can't parse the input file: " + e.getClass().getSimpleName() + " " + e.getMessage());
        }
    }

    @Override
    public Feature lookup(String ngram) {
        return features.get(ngram);
    }

    @Override
    public List<Feature> lookup(List<String> ngrams) {
        return ngrams.stream().map(this::lookup).collect(Collectors.toList());
    }
}
