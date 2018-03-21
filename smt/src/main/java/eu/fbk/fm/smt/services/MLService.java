package eu.fbk.fm.smt.services;

import eu.fbk.fm.alignments.scorer.text.*;
import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.analysis.stemmer.Stemmer;
import eu.fbk.utils.analysis.stemmer.StemmerFactory;
import eu.fbk.utils.analysis.stemmer.StemmerNotFoundException;
import eu.fbk.utils.lsa.LSM;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Provides machine learning related objects
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service @Singleton
public class MLService {
    private boolean turnOffStemmer = true;
    private VectorProvider bowProvider = null;
    private VectorProvider lsaProvider = null;
    private VectorProvider bowClaudioProvider = null;

    @Inject
    private NGramsService ngrams;

    @Inject
    @Named("lsaFilename")
    private String lsaFilename;

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

    public synchronized void initLSA() throws IOException {
        if (lsaProvider == null) {
            LSAVectorProvider lsaProvider = new LSAVectorProvider(new LSM(lsaFilename, 100, true));
            this.lsaProvider = lsaProvider;
            this.bowClaudioProvider = lsaProvider.getBOWProvider();
        }
    }

    public synchronized void initBOW() {
        if (bowProvider == null) {
            bowProvider = new BOWVectorProvider(provideFeatureExtraction(), ngrams);
        }
    }

    public synchronized VectorProvider provideBOWVectors() {
        initBOW();
        return bowProvider;
    }

    public VectorProvider provideLSAVectors() throws IOException {
        initLSA();
        return lsaProvider;
    }

    public VectorProvider provideBOWClaudioVectors() throws IOException {
        initLSA();
        return bowClaudioProvider;
    }

    public SimilarityScorer[] getScorers() throws IOException {
        initLSA();
        initBOW();
        return new SimilarityScorer[]{
            new CosineScorer(bowProvider.debug()),
            new CosineScorer(lsaProvider.debug()),
            new CosineScorer(bowClaudioProvider.debug())
        };
    }

    public SimilarityScorer getDefaultScorer() throws IOException {
        initLSA();
        return new CosineScorer(lsaProvider);
    }

    public MLService turnOffStemmer() {
        turnOffStemmer = true;
        return this;
    }

    public NGramsService getNgrams() {
        return ngrams;
    }
}
