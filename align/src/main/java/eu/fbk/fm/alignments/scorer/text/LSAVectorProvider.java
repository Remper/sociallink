package eu.fbk.fm.alignments.scorer.text;

import eu.fbk.utils.lsa.BOW;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.Vector;

import java.util.HashMap;

/**
 * Provides LSA-based vector representation of the document
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class LSAVectorProvider implements VectorProvider {
    protected LSM lsa;

    public LSAVectorProvider(LSM lsa) {
        this.lsa = lsa;
    }

    protected Vector toBOW(String text) {
        BOW bow = new BOW(text);
        debug("bow", bow.termSet());
        return lsa.mapDocument(bow);
    }

    @Override
    public Vector toVector(String text) {
        return lsa.mapPseudoDocument(toBOW(text));
    }

    @Override
    public String toString() {
        return "lsa";
    }

    public BOWVectorProvider getBOWProvider() {
        return new BOWVectorProvider(lsa);
    }

    protected void debug(String key, Object value) { }

    /**
     * LSA also contains it's own BOW model
     */
    public static class BOWVectorProvider extends LSAVectorProvider {

        public BOWVectorProvider(LSM lsa) {
            super(lsa);
        }

        @Override
        public DebuggableVectorProvider debug() {
            return new DebugBOW(lsa);
        }

        @Override
        public Vector toVector(String text) {
            return toBOW(text);
        }

        @Override
        public String toString() {
            return "bow_claudio";
        }
    }

    @Override
    public DebuggableVectorProvider debug() {
        return new Debug(lsa);
    }

    public static class Debug extends LSAVectorProvider implements DebuggableVectorProvider {
        HashMap<String, Object> debug = new HashMap<>();

        public Debug(LSM lsa) {
            super(lsa);
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

    public static class DebugBOW extends BOWVectorProvider implements DebuggableVectorProvider {
        HashMap<String, Object> debug = new HashMap<>();

        public DebugBOW(LSM lsa) {
            super(lsa);
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
}
