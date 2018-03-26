package eu.fbk.fm.profiling.extractors.LSA;

import org.apache.log4j.Logger;

import java.text.BreakIterator;
import java.util.*;

/**
 * Bag-of-word representation of a document. For each term
 * has the term-frequency.
 *
 * @author Claudio Giuliano
 * @version %I%, %G%
 * @since 1.0
 */
public class BOW {

    /**
     * Define a static logger variable so that it references the
     * Logger instance named <code>BOW</code>.
     */
    static Logger logger = Logger.getLogger(BOW.class.getName());

    public static final int RAW_TERM_FREQUENCY = 0;

    public static final int BOOLEAN_TERM_FREQUENCY = 1;

    public static final int LOGARITHMIC_TERM_FREQUENCY = 2;

    public static final int AUGMENTED_TERM_FREQUENCY = 3;

    public static final int DEFAULT_TERM_FREQUENCY_TYPE = LOGARITHMIC_TERM_FREQUENCY;

    public static final String[] labels = { "raw", "boolean", "log", "augmented" };

    private Map<String, Counter> map;

    protected int max;

    private int termFrequencyType;

    public BOW() {
        this(LOGARITHMIC_TERM_FREQUENCY);
    }

    public BOW(int termFrequencyType) {
        map = new HashMap<>();
        max = 1;
        this.termFrequencyType = termFrequencyType;
    }

    public BOW(String text) {
        this(LOGARITHMIC_TERM_FREQUENCY);
        tokenize(text);
    }

    public int getMax() {
        return max;
    }

    public int getTermFrequencyType() {
        return termFrequencyType;
    }

    public void setTermFrequencyType(int termFrequencyType) {
        this.termFrequencyType = termFrequencyType;
    }

    public double tf(String term) {

        switch (termFrequencyType) {
            case RAW_TERM_FREQUENCY:
                return rawFrequency(term);
            case BOOLEAN_TERM_FREQUENCY:
                return booleanFrequency(term);
            case AUGMENTED_TERM_FREQUENCY:
                return augmentedFrequency(term);
            case LOGARITHMIC_TERM_FREQUENCY:
                return logarithmicFrequency(term);

        }
        return logarithmicFrequency(term);
    }

    public int booleanFrequency(String term) {
        Counter c = map.get(term);
        if (c == null) {
            return 0;
        }

        return 1;
    }

    public int rawFrequency(String term) {
        Counter c = map.get(term);
        if (c == null) {
            return 0;
        }

        return c.get();
    }

    public static final double LOG2 = Math.log(2);

    public double log2(double d) {
        return Math.log(d) / LOG2;
    }

    public double logarithmicFrequency(String term) {
        Counter c = map.get(term);
        if (c == null) {
            return 0;
        }
        int tf = c.get();
        if (tf == 1) {
            return 1;
        }
        return log2(c.get() + 1);
    }

    public double augmentedFrequency(String term) {
        //logger.debug(term + "\tmax=" + max);
        Counter c = map.get(term);
        if (c == null) {
            return 0;
        }
        //logger.debug(term + "\tratio=" + ((double) c.get() / max));
        double tf = 0.5 + (0.5 * ((double) c.get() / max));
        //logger.debug(term + "\t0.5+(0.5*(" + c.get() + "/" + max + "))=" + tf);
        return tf;
    }

    // a tokenized string
    public BOW(String[] text) {
        map = new HashMap<>();
        for (int i = 0; i < text.length; i++) {
            //add(text[i].toLowerCase());
            if (text[i].length() > 0) {
                //logger.debug(i + "\t" + text[i]);
                add(text[i]);
            }

        }
    }

    // TODO: use a tokenizer from jcore analysis
    private void tokenize(String text) {

        BreakIterator boundary = BreakIterator.getWordInstance(Locale.US);
        boundary.setText(text);
        int start = boundary.first();
        String token = null;

        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            token = text.substring(start, end).toLowerCase();

            if (!token.matches("\\s+")) {
                //logger.info("'" + token + "'\t" + start + ", " + end);
                add(token);
            }
        } // end for end

    }

    public void add(BOW bow) {
        for (String term : bow.termSet()) {
            int count = map.containsKey(term) ? map.get(term).get() : 0;
            count += bow.getFrequency(term);

            if (count > max) {
                max = count;
            }
            map.put(term, new Counter(count));
        }
    }

    public void addAll(String[] words, int from, int to) {
        //logger.debug(Arrays.toString(words));
        for (int i = from; i < to; i++) {
            add(words[i]);
        }
    }

    public void addAll(String[] words) {
        //logger.debug(Arrays.toString(words));
        for (int i = 0; i < words.length; i++) {
            add(words[i]);
        }
    }

    public int add(String word) {

        Counter c = map.get(word);
        //logger.debug("adding " + word+"\t"+c);
        if (c == null) {
            map.put(word, new Counter(1));
            return 1;
        }

        int value = c.incAndGet();
        //logger.debug(word + "\tvalue ="+ value);
        if (value > max) {
            max = value;
        }
        //logger.debug("max = " + max);
        return value;
    }

    public boolean contains(String word) {
        Counter c = map.get(word);
        if (c == null) {
            return false;
        }

        return true;
    }

    public Set<String> termSet() {
        return map.keySet();
    }

    public Iterator<String> iterator() {
        return map.keySet().iterator();
    }

    public int getFrequency(String word) {
        Counter c = map.get(word);
        if (c == null) {
            return 0;
        }

        return c.get();
    }

    public int size() {
        return map.size();
    }

    SortedMap<Integer, List<String>> getSortedMap() {
        SortedMap<Integer, List<String>> sortedMap = new TreeMap<Integer, List<String>>(new IntegerComparator());
        Iterator<String> it = iterator();
        String w;
        List<String> list;
        int f;
        for (int i = 0; it.hasNext(); i++) {
            w = it.next();
            f = getFrequency(w);
            list = sortedMap.get(f);
            if (list == null) {
                list = new ArrayList<String>();
                sortedMap.put(f, list);
            }
            list.add(w);
        }
        return sortedMap;
    } // end sort

    class IntegerComparator implements Comparator<Integer> {

        //
        public int compare(Integer o1, Integer o2) {
            //logger.debug("comparing " + o1 + ", " + o2);

            int diff = o1.intValue() - o2.intValue();

            if (diff == 0) {
                return 0;
            } else if (diff < 0) {
                return 1;
            }

            return -1;

        } // end compare
    }

    //
    public String toSortedLine() {
        SortedMap<Integer, List<String>> sortedMap = getSortedMap();
        StringBuilder sb = new StringBuilder();
        Iterator<Integer> it = sortedMap.keySet().iterator();
        List<String> list;
        String w;
        int f;
        boolean b = true;
        while (it.hasNext()) {
            f = it.next();
            list = sortedMap.get(f);
            for (int i = 0; i < list.size(); i++) {
                w = list.get(i);
                if (b) {
                    b = false;
                } else {
                    sb.append(" ");
                }
                sb.append(w);
                sb.append(":");
                sb.append(f);
            }

        }

        return sb.toString();
    }

    //
    public String toSingleLine() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> it = iterator();
        String w;
        int f;
        if (it.hasNext()) {
            w = it.next();
            f = getFrequency(w);
            sb.append(w);
            sb.append(":");
            sb.append(f);
        }
        while (it.hasNext()) {
            w = it.next();
            f = getFrequency(w);
            sb.append(" ");
            sb.append(w);
            sb.append(":");
            sb.append(f);
        } // end while
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> it = iterator();
        sb.append("bow(\n");
        while (it.hasNext()) {
            String w = it.next();
            sb.append(w);
            sb.append("\t");
            sb.append(getFrequency(w));
            sb.append("\t");
            sb.append(rawFrequency(w));
            sb.append("\t");
            sb.append(logarithmicFrequency(w));
            sb.append("\t");
            sb.append(booleanFrequency(w));
            sb.append("\t");
            sb.append(augmentedFrequency(w));
            sb.append("\n");
            /*
				if (d > maxScore)
			{
					maxScore = d;
					maxObjext = word;
			}*/
        } // end while

        sb.append(")(max = ");
        sb.append(max);
        sb.append(")\n");
		/*
		 sb.append("\nmax: ");
		 sb.append(maxObjext);
		 sb.append("\t");
		 sb.append(maxScore);
		 sb.append("\n");*/

        return sb.toString();
    }

    public Set<String> keySet() {

        return map.keySet();
    }

    //
    class Counter {

        int count;

        public Counter(int count) {
            this.count = count;
        }

        public void inc() {
            count++;
        }

        public int incAndGet() {
            return ++count;
        }

        public int get() {
            return count;
        }

        @Override
        public String toString() {
            return new Integer(count).toString();
        }
    }
}