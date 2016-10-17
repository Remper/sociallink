package eu.fbk.fm.alignments;

import java.io.File;

/**
 * Provides file objects for the common filename structure
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FileProvider {
    public File gold, resolved, scaler, model, evaluation, evaluationResult;
    public FileSet test, train;

    public FileProvider(String workdir) {
        File coreDirectory = new File(workdir);
        gold = new File(coreDirectory, "gold.csv");
        resolved = new File(coreDirectory, "resolved.json");
        scaler = new File(coreDirectory, "scaler.json");
        model = new File(coreDirectory, "model");
        test = new FileSet(coreDirectory, "test");
        train = new FileSet(coreDirectory, "train");
        evaluation = new File(coreDirectory, "evaluation.json");
        evaluationResult = new File(coreDirectory, "evaluation.txt");

        if (!coreDirectory.exists() || !coreDirectory.isDirectory()) {
            throw new IllegalArgumentException("Target directory doesn't exist or isn't a directory");
        }
    }

    public static class FileSet {
        public File plain;
        public FeatureSet scaled, unscaled;

        private FileSet(File coreDirectory, String prefix) {
            plain = new File(coreDirectory, prefix + ".csv");
            scaled = new FeatureSet(coreDirectory, prefix);
            unscaled = new FeatureSet(coreDirectory, prefix + ".unscaled");
        }
    }

    public static class FeatureSet {
        public File SVMFeat, CSVFeat, index;
        public File CSVContrastive;

        private FeatureSet(File coreDirectory, String prefix) {
            SVMFeat = new File(coreDirectory, prefix + ".feat.svm");
            CSVFeat = new File(coreDirectory, prefix + ".feat.csv");
            index = new File(coreDirectory, prefix + ".index.csv");
            CSVContrastive = new File(coreDirectory, prefix + ".feat.contrastive.csv");
        }
    }
}
