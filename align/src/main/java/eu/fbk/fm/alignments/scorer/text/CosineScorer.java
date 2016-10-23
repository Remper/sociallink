package eu.fbk.fm.alignments.scorer.text;

import eu.fbk.utils.math.Vector;

import java.util.HashMap;

/**
 * Scores two texts calculating cosine similarity using the provided vector model
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class CosineScorer implements SimilarityScorer {
    private VectorProvider provider;

    public CosineScorer(VectorProvider provider) {
        this.provider = provider;
    }

    @Override
    public double score(String text1, String text2) {
        Vector v1 = this.provider.toVector(text1);
        debug("v1count", v1.elementCount());
        if (this.provider instanceof Debuggable) {
            debug("v1text2vec", ((Debuggable) this.provider).dump());
        }
        Vector v2 = this.provider.toVector(text2);
        debug("v2count", v2.elementCount());
        if (this.provider instanceof Debuggable) {
            debug("v2text2vec", ((Debuggable) this.provider).dump());
        }

        return v1.dotProduct(v2) / Math.sqrt(v1.dotProduct(v1) * v2.dotProduct(v2));
    }

    protected void debug(String key, Object value) { }

    @Override
    public DebuggableSimilarityScorer debug() {
        return new Debug(provider);
    }

    public static class Debug extends CosineScorer implements DebuggableSimilarityScorer {
        HashMap<String, Object> debug = new HashMap<>();

        public Debug(VectorProvider provider) {
            super(provider);
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
        return "cos_sim+" + this.provider.toString();
    }
}
