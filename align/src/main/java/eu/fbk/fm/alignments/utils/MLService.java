package eu.fbk.fm.alignments.utils;

import eu.fbk.fm.alignments.persistence.NGramsService;
import eu.fbk.fm.ml.TextSimilarity;
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
    private TextSimilarity similarity = null;

    private ResourcesService resources;

    private NGramsService ngrams;

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

    public synchronized TextSimilarity provideTextSimilarity() throws Exception {
        if (similarity == null) {
            FeatureExtraction extraction = provideFeatureExtraction();
            //FeatureMapping mapping = resources.provideNGrams(new DatasetRepository(new Configuration()));
            similarity = new TextSimilarity(extraction, ngrams);
        }
        return similarity;
    }

    public MLService turnOffStemmer() {
        turnOffStemmer = true;
        return this;
    }
}
