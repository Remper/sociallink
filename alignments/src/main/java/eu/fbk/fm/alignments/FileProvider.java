package eu.fbk.fm.alignments;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

/**
 * Provides file objects for the common filename structure
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FileProvider {
    public final File
        gold, goldFiltered, input,
        trueDist, totalDist, strategyCheck,
        resolved, manifest,
        dataset, datasetStats, evaluationResult, evaluationRawResult;

    public FileProvider(String workdir) {
        this(new File(workdir));
    }

    public FileProvider(File coreDirectory) {
        if (!coreDirectory.exists() || !coreDirectory.isDirectory()) {
            throw new IllegalArgumentException("Target directory doesn't exist or isn't a directory");
        }
        Function<String, File> folder = folder(coreDirectory);
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM-dd-HH-mm"));

        gold = folder.apply("gold.csv");
        input = folder.apply("input.csv");
        goldFiltered = folder.apply("gold_filtered.csv");
        resolved = folder.apply("resolved");
        manifest = folder.apply("manifest.json");
        trueDist = folder.apply("ca-true-distribution.txt");
        totalDist = folder.apply("ca-total-distribution.txt");
        strategyCheck = folder.apply("strategy-check.txt");
        dataset = folder.apply("dataset.json");
        datasetStats = folder.apply("stats.tsv");
        evaluationResult = folder.apply("evaluation-"+date+".txt");
        evaluationRawResult = folder.apply("evaluation-raw-"+date);
    }

    public File getEvaluationRawResultFile(boolean joint, String type) {
        return new File(evaluationRawResult, String.format("%s-%s.txt", type, joint ? "joint" : "selection"));
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
        public File JSONFeat, index, JSONJointFeat;

        private FeatureSet(File coreDirectory, String prefix) {
            JSONFeat = new File(coreDirectory, prefix + ".feat.json");
            JSONJointFeat = new File(coreDirectory, prefix + ".joint.feat.json");
            index = new File(coreDirectory, prefix + ".index.csv");
        }
    }

    public static Function<String, File> folder(File parent) {
        return (child) -> new File(parent, child);
    }
}
