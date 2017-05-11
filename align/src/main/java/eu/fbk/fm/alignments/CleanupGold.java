package eu.fbk.fm.alignments;

import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.pipeline.SubmitEntities;
import eu.fbk.utils.core.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Only leaves persons and organisations in the gold standard
 */
public class CleanupGold {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubmitEntities.class);
    private static final String ENDPOINT = "endpoint";
    private static final String WORKING = "work";

    private final Endpoint endpoint;

    public CleanupGold(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void run(String input) throws IOException {
        FileWriter writer = new FileWriter(input+"_out");
        CleanupGold script = this;
        Files.lines(Paths.get(input)).parallel().forEach(line -> {
            DBpediaResource resource = endpoint.getResourceById(line.split(",")[0]);
            if (!resource.isPerson() && !resource.isCompany()) {
                return;
            }

            synchronized (script) {
                try {
                    writer.write(line);
                    writer.write('\n');
                } catch(Exception e) {
                    LOGGER.error("Error while writing", e);
                }
            }
        });
        writer.close();
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption(null, ENDPOINT,
                        "URL to SPARQL endpoint", "ENDPOINT",
                        CommandLine.Type.STRING, true, false, true)
                .withOption("w", WORKING,
                        "Input file with entities", "INPUT",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String endpointUri = cmd.getOptionValue(ENDPOINT, String.class);
            final String workingFile = cmd.getOptionValue(WORKING, String.class);

            Endpoint endpoint = new Endpoint(endpointUri);
            CleanupGold script = new CleanupGold(endpoint);

            script.run(workingFile);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
