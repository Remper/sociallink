package eu.fbk.fm.profiling;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import eu.fbk.fm.alignments.index.BuildUserLSA;
import eu.fbk.utils.core.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Reads the filtered twitter stream, groups by a person and extracts features
 */
public class GroupAndExtractFeatures {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupAndExtractFeatures.class);

    private static final String RESULTS_PATH = "results-path";
    private static final String LIST_PATH = "list";
    private static final String TWEETS_PATH = "tweets-path";

    private String[] uids;

    public GroupAndExtractFeatures(String[] uids) {
        this.uids = uids;
    }

    public void start(String inputPath, String outputPath) throws Exception {
        Files.fileTraverser();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("t", TWEETS_PATH,
                        "specifies the directory from which to get a stream of tweets", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("r", RESULTS_PATH,
                        "specifies the directory to which the results will be saved", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, false)
                .withOption("l", LIST_PATH,
                        "specifies the file with the list of user handlers to filter", "FILE",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final String listPath = cmd.getOptionValue(LIST_PATH, String.class);
            //noinspection ConstantConditions
            final String tweetsPath = cmd.getOptionValue(TWEETS_PATH, String.class);
            final String resultsPath;
            if (cmd.hasOption(RESULTS_PATH)) {
                resultsPath = cmd.getOptionValue(RESULTS_PATH, String.class);
            } else {
                resultsPath = tweetsPath;
            }

            String[] uids = Files.asCharSource(new File(listPath), Charsets.UTF_8)
                    .readLines().stream()
                    .map(line -> line.split(",")[1])
                    .toArray(String[]::new);
            LOGGER.info(String.format("Loaded %d uids", uids.length));

            GroupAndExtractFeatures extractor = new GroupAndExtractFeatures(uids);
            extractor.start(tweetsPath, resultsPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
