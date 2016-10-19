package eu.fbk.ict.fm.smt.services;

import eu.fbk.fm.ml.TextSimilarity;
import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.analysis.stemmer.Stemmer;
import eu.fbk.utils.analysis.stemmer.StemmerFactory;
import eu.fbk.utils.analysis.stemmer.StemmerNotFoundException;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Provides machine learning related objects
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service @Singleton
public class MLService {
    private boolean turnOffStemmer = true;
    private TextSimilarity similarity = null;

    @Inject
    private ResourcesService resources;

    @Inject
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

    public synchronized TextSimilarity provideTextSimilarity() {
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
