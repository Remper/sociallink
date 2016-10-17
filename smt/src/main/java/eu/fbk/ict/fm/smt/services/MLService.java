package eu.fbk.ict.fm.smt.services;

import eu.fbk.ict.fm.data.Configuration;
import eu.fbk.ict.fm.data.DatasetRepository;
import eu.fbk.ict.fm.data.dataset.FeatureMapping;
import eu.fbk.ict.fm.data.ngrams.NGramsService;
import eu.fbk.ict.fm.ml.TextSimilarity;
import eu.fbk.ict.fm.ml.features.FeatureExtraction;
import org.fbk.cit.hlt.core.analysis.stemmer.Stemmer;
import org.fbk.cit.hlt.core.analysis.stemmer.StemmerFactory;
import org.fbk.cit.hlt.core.analysis.stemmer.StemmerNotFoundException;
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
