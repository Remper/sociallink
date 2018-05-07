package eu.fbk.fm.vectorize.preprocessing.text;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import eu.fbk.utils.core.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;
import org.tensorflow.example.Features;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Misc class for validating the output of PopulateCooccurrenceMatrix
 */
public class ValidateCooccurrenceShards {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidateCooccurrenceShards.class);

    private static final String SHARD_PATH = "shard-path";

    public void checkShards(File shardPath) {
        checkArgument(shardPath.exists() && shardPath.isDirectory(), "Shards directory should exist");

        int total = 0;
        List<String> failures = new LinkedList<>();
        for (File file : shardPath.listFiles()) {
            if (!file.getName().endsWith(".pb")) {
                continue;
            }
            total++;
            try {
                Features features = Example.parseFrom(Files.asByteSource(file).read()).getFeatures();
                features.getFeatureOrThrow("global_row");
                features.getFeatureOrThrow("global_col");
                features.getFeatureOrThrow("sparse_local_row");
                features.getFeatureOrThrow("sparse_local_col");
                features.getFeatureOrThrow("sparse_value");
            } catch (IOException e) {
                LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
                failures.add(file.getName());
            }
        }

        LOGGER.info("Stats");
        LOGGER.info("  total shards #: "+total);
        LOGGER.info("  failed shards #: "+failures.size());
        if (failures.size() > 0) {
            LOGGER.info("  failed shards: "+String.join(", ", failures));
        }
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("s", SHARD_PATH,
                        "shard directory", "DIRECTORY",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        ValidateCooccurrenceShards extractor = new ValidateCooccurrenceShards();

        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final File shardPath = new File(cmd.getOptionValue(SHARD_PATH, String.class));

            extractor.checkShards(shardPath);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
