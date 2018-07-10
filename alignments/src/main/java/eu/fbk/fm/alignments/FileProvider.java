package eu.fbk.fm.alignments;

import com.google.gson.reflect.TypeToken;
import eu.fbk.utils.math.Scaler;

import java.io.File;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Provides file objects for the common filename structure
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FileProvider {
    public final File gold, resolved, trueDist, totalDist, manifest, dataset, datasetStats, evaluationResult, evaluationRawResult;
    public final Type scalerType = new TypeToken<Map<String, Scaler>>(){}.getType();

    public FileProvider(String workdir) {
        this(new File(workdir));
    }

    public FileProvider(File coreDirectory) {
        if (!coreDirectory.exists() || !coreDirectory.isDirectory()) {
            throw new IllegalArgumentException("Target directory doesn't exist or isn't a directory");
        }

        gold = new File(coreDirectory, "gold.csv");
        resolved = new File(coreDirectory, "resolved");
        manifest = new File(coreDirectory, "manifest.json");
        trueDist = new File(coreDirectory, "ca-true-distribution.txt");
        totalDist = new File(coreDirectory, "ca-total-distribution.txt");
        dataset = new File(coreDirectory, "dataset.json");
        datasetStats = new File(coreDirectory, "stats.tsv");
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM-dd-HH-mm"));
        evaluationResult = new File(coreDirectory, "evaluation-"+date+".txt");
        evaluationRawResult = new File(coreDirectory, "evaluation-raw-"+date);
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
}
