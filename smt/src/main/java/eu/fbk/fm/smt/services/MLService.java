package eu.fbk.fm.smt.services;

import eu.fbk.fm.alignments.scorer.text.*;
import eu.fbk.fm.ml.features.FeatureExtraction;
import eu.fbk.utils.analysis.stemmer.Stemmer;
import eu.fbk.utils.analysis.stemmer.StemmerFactory;
import eu.fbk.utils.analysis.stemmer.StemmerNotFoundException;
import eu.fbk.utils.lsa.LSM;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;

/**
 * Provides machine learning related objects
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class MLService {
    private VectorProvider bowProvider = null;
    private VectorProvider lsaProvider = null;
    private VectorProvider lsaBowProvider = null;

    @Inject
    @Named("lsaFilename")
    private String lsaFilename;

    public synchronized void initLSA() throws IOException {
        if (lsaProvider == null) {
            LSM lsm = new LSM(lsaFilename+"/X", 100, true);
            this.lsaProvider = new LSAVectorProvider(lsm);
            this.bowProvider = new LSAVectorProvider.BOWVectorProvider(lsm);
            this.lsaBowProvider = new LSAVectorProvider.LSABOWVectorProvider(lsm);
        }
    }

    public VectorProvider provideLSAVectors() throws IOException {
        initLSA();
        return lsaProvider;
    }

    public SimilarityScorer[] getScorers() throws IOException {
        initLSA();
        return new SimilarityScorer[]{
            new CosineScorer(bowProvider),
            new CosineScorer(lsaProvider),
            new CosineScorer(lsaBowProvider)
        };
    }

    public SimilarityScorer getDefaultScorer() throws IOException {
        initLSA();
        return new CosineScorer(lsaProvider);
    }
}
