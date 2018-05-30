package eu.fbk.fm.alignments.scorer.text;

import eu.fbk.utils.lsa.BOW;
import eu.fbk.utils.lsa.LSM;
import eu.fbk.utils.math.DenseVector;
import eu.fbk.utils.math.SparseVector;
import eu.fbk.utils.math.Vector;

import java.util.HashMap;
import java.util.Iterator;

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

    /**
     * LSA+BOW model
     */
    public static class LSABOWVectorProvider extends LSAVectorProvider {

        public LSABOWVectorProvider(LSM lsa) {
            super(lsa);
        }

        @Override
        public Vector toVector(String text) {
            Vector bowVector = toBOW(text);
            //Merging with LSA vector first to have a correct and consistent merging
            return merge(lsa.mapPseudoDocument(bowVector), bowVector);
        }

        private Vector merge(Vector first, Vector second) {
            Vector m = new SparseVector();

            Iterator<Integer> it1 = first.nonZeroElements();
            int i;
            while (it1.hasNext()) {
                i = it1.next();
                m.add(i, first.get(i));
            }

            Iterator<Integer> it2 = second.nonZeroElements();
            while (it2.hasNext()) {
                i = it2.next();
                m.add(i + first.size(), second.get(i));
            }

            return m;
        }

        @Override
        public String toString() {
            return "bow+lsa";
        }
    }

    /**
     * LSA also contains it's own BOW model
     */
    public static class BOWVectorProvider extends LSAVectorProvider {

        public BOWVectorProvider(LSM lsa) {
            super(lsa);
        }

        @Override
        public Vector toVector(String text) {
            return toBOW(text);
        }

        @Override
        public String toString() {
            return "bow";
        }
    }
}
