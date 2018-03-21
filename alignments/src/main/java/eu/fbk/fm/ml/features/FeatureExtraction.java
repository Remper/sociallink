package eu.fbk.fm.ml.features;

import eu.fbk.utils.analysis.stemmer.Stemmer;
import eu.fbk.utils.analysis.tokenizer.HardTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Processes raw text and extracts features
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FeatureExtraction {
    private static final Logger logger = LoggerFactory.getLogger(FeatureExtraction.class);

    public static final int DEFAULT_MAX_N_GRAMS_LENGTH = 1;
    public static final boolean DEFAULT_SPARSE = false;

    private TermSet blackList, whiteList;
    private Stemmer stemmer;
    private int maxNGramsLength;
    private boolean sparse;
    private String language;

    public FeatureExtraction() {
        this(DEFAULT_MAX_N_GRAMS_LENGTH, DEFAULT_SPARSE);
    }

    public FeatureExtraction(int maxNGramsLength, boolean sparse) {
        this.maxNGramsLength = maxNGramsLength;
        this.sparse = sparse;
    }

    private boolean checkBlackList(String[] tokens) {
        traceTokens(tokens, "checking");
        if (blackList == null) {
            return false;
        }
        for (String token : tokens) {
            if (blackList.contains(token)) {
                traceTokens(tokens, "found");
                return true;
            }
        }

        traceTokens(tokens, "not found");
        return false;
    }

    private boolean checkWhiteListUnigrams(String[] tokens) {
        traceTokens(tokens, "checking");
        if (whiteList == null) {
            return false;
        }
        for (String token : tokens) {
            if (!whiteList.contains(token)) {
                traceTokens(tokens, "not found");
                return true;
            }
        }

        traceTokens(tokens, "found");
        return false;
    }

    private boolean checkWhiteListNgrams(String[] tokens) {
        traceTokens(tokens, "checking");
        if (whiteList == null) {
            return false;
        }

        if (!whiteList.contains(constructNgram(tokens))) {
            traceTokens(tokens, "not found");
            return true;
        }

        traceTokens(tokens, "found");
        return false;
    }

    private static void traceTokens(String[] tokens, String message) {
        if (logger.isTraceEnabled()) {
            logger.trace(Arrays.toString(tokens) + " " + message);
        }
    }

    private static String constructNgram(String[] tokens) {
        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            if (sb.length() > 0) {
                sb.append("_");
            }
            sb.append(token);
        }
        return sb.toString();
    }

    private void addToSet(Set<String> features, String... ngram) {
        if (checkBlackList(ngram)) {
            traceTokens(ngram, "in black list");
            return;
        }
        if (checkWhiteListNgrams(ngram) && checkWhiteListUnigrams(ngram)) {
            traceTokens(ngram, "not in white list");
            return;
        }
        logger.trace("YES\t" + Arrays.toString(ngram));
        features.add(constructNgram(ngram));
    }

    private final static char[] specialCharacters = new char[]{
            '!', '?', '\'', '(', ')', '-', ':', ';'
    };

    private static boolean isWord(String w) {
        char ch;
        for (int i = 0; i < w.length(); i++) {
            ch = w.charAt(i);
            //todo: rethink this it removes emoticons
            if (!Character.isLetter(ch) && !isSpecialCharacter(ch)) {
                logger.trace("NO word\t" + w);
                return false;
            }

        }
        logger.trace("YES word\t" + w);
        return true;
    }

    private static boolean isSpecialCharacter(char ch) {
        for (char chS : specialCharacters) {
            if (chS == ch) {
                return true;
            }
        }
        return false;
    }

    private void stem(String[] tokens) {
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = stem(tokens[i]);
        }
    }

    private String stem(String input) {
        if (stemmer != null) {
            return stemmer.stem(input);
        }
        return input;
    }

    // adds unigrams
    private void addUnigrams(Set<String> features, String[] tokens) {
        for (String token : tokens) {
            addToSet(features, token);
        }
    }

    // adds (sparse) bigrams
    private void addBigrams(Set<String> features, String[] tokens, int j) {
        for (int i = 0; i < tokens.length - j; i++) {
            addToSet(features, tokens[i], tokens[i + j]);
        }
    }

    //adds trigrams
    private void addTrigrams(Set<String> features, String[] tokens, int k, int j) {
        for (int i = 0; i < tokens.length - j; i++) {
            addToSet(features, tokens[i], tokens[i + k], tokens[i + j]);
        }
    }

    /**
     * Returns a set containing the negations found in the array of tokens
     *
     * @param tokens an array of tokens
     * @return a set containing the negations
     */
    private Set<String> extractNegativeTermSet(String[] tokens) {
        Set<String> set = new HashSet<>();
        for (String token : tokens) {
            if (isNegative(token)) {
                set.add(stem(token));
            }
        }
        return set;
    }

    private static final String[] negatives = new String[]{
            "non", "nn", "poco", "pochissimo", "nulla", "niente", "neppure",
            "né", "senza", "manco", "nemmeno", "meno", "mica", "mai"
    };

    private static boolean isNegative(String token) {
        for (String negative : negatives) {
            if (token.equals(negative)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> extract(String s) {
        logger.trace("extracting from \"" + s + "\"...");

        Set<String> features = new HashSet<>();
        HardTokenizer tokenizer = new HardTokenizer();

        String[] tokens = tokenizer.stringArray(s.toLowerCase());
        logger.trace(tokens.length + " tokens (" + maxNGramsLength + ") " + Arrays.toString(tokens));


        if (stemmer != null) {
            stem(tokens);
        }

        if (maxNGramsLength > 0) {
            addUnigrams(features, tokens);
        }

        if (maxNGramsLength > 1) {
            // contiguous bigrams (a b)
            addBigrams(features, tokens, 1);
            if (sparse) {
                //sparse bigrams  (a ... b)
                addBigrams(features, tokens, 2);
                addBigrams(features, tokens, 3);
                addBigrams(features, tokens, 4);
                addBigrams(features, tokens, 5);
                addBigrams(features, tokens, 6);
                addBigrams(features, tokens, 7);
            }
        }

        if (maxNGramsLength > 2) {
            // contiguous trigrams (a b c)
            addTrigrams(features, tokens, 1, 2);
            if (sparse) {
                //sparse trigrams (a b ... c, a ... b c)
                addTrigrams(features, tokens, 1, 3);
                addTrigrams(features, tokens, 2, 3);
                addTrigrams(features, tokens, 1, 4);
                addTrigrams(features, tokens, 3, 4);
            }
        }

        logger.trace("features " + features);
        return features;
    }

    public void interactive() throws Exception {
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader myInput = new BufferedReader(isr);

        while (true) {
            System.out.println("\nPlease write a text and type <return> to continue (CTRL C to exit):");
            String query = myInput.readLine();
            Set<String> features = extract(query);
            logger.info(features.size() + "\t" + features);
        }
    }

    public TermSet getBlackList() {
        return blackList;
    }

    public void setBlackList(TermSet blackList) {
        this.blackList = blackList;
    }

    public TermSet getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(TermSet whiteList) {
        this.whiteList = whiteList;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public int getMaxNGramsLength() {
        return maxNGramsLength;
    }

    public void setMaxNGramsLength(int maxNGramsLength) {
        this.maxNGramsLength = maxNGramsLength;
    }

    public boolean isSparse() {
        return sparse;
    }

    public void setSparse(boolean sparse) {
        this.sparse = sparse;
    }

    public Stemmer getStemmer() {
        return stemmer;
    }

    public void setStemmer(Stemmer stemmer) {
        this.stemmer = stemmer;
    }
}


