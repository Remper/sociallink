package eu.fbk.fm.alignments.utils;

import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.analysis.stemmer.Stemmer;
import eu.fbk.utils.analysis.stemmer.StemmerFactory;
import eu.fbk.utils.analysis.stemmer.StemmerNotFoundException;

/**
 * Provides machine learning related objects
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class MLService {
    private boolean turnOffStemmer = true;

    public FeatureExtraction provideFeatureExtraction() {
        FeatureExtraction extraction = new FeatureExtraction();
        extraction.setSparse(true);
        extraction.setMaxNGramsLength(2);
        if (!turnOffStemmer) {
            Stemmer stemmer = null;
            try {
                stemmer = StemmerFactory.getInstance("en");
            } catch (StemmerNotFoundException e) {
                e.printStackTrace();
            }
            if (stemmer != null) {
                extraction.setStemmer(stemmer);
                extraction.setLanguage("en");
            }
        }
        return extraction;
    }

    public MLService turnOffStemmer() {
        turnOffStemmer = true;
        return this;
    }
}
