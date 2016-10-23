package eu.fbk.fm.alignments.scorer.text;

import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.data.dataset.bow.FeatureMapping;
import eu.fbk.utils.data.dataset.bow.FeatureMappingInterface;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.SparseVector;
import eu.fbk.utils.math.Vector;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * BOW model based on FeatureExtraction tokenizer and FeatureMapping weights
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class BOWVectorProvider implements VectorProvider {
    private FeatureExtraction extractor;
    private FeatureMappingInterface mapping;

    public BOWVectorProvider(FeatureExtraction extractor, FeatureMappingInterface mapping) {
        this.extractor = extractor;
        this.mapping = mapping;
    }

    @Override
    public Vector toVector(String text) {
        SparseVector vector = new SparseVector();

        Set<String> termSet = extractor.extract(text);
        debug("bow", termSet);
        List<FeatureMapping.Feature> features = mapping.lookup(termSet.stream().collect(Collectors.toList()));
        for (FeatureMapping.Feature token : features) {
            if (token == null) {
                continue;
            }

            vector.add(token.index, (float) token.weight);
        }
        return vector;
    }

    protected void debug(String key, Object value) { }

    @Override
    public DebuggableVectorProvider debug() {
        return new DebugBOW(extractor, mapping);
    }

    public static class DebugBOW extends BOWVectorProvider implements DebuggableVectorProvider {
        HashMap<String, Object> debug = new HashMap<>();

        public DebugBOW(FeatureExtraction extractor, FeatureMappingInterface mapping) {
            super(extractor, mapping);
        }

        @Override
        protected void debug(String key, Object value) {
            debug.put(key, value);
        }

        @Override
        public Object dump() {
            HashMap<String, Object> debug = this.debug;
            this.debug = new HashMap<>();
            return debug;
        }
    }

    @Override
    public String toString() {
        return "bow";
    }
}
