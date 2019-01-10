package eu.fbk.fm.alignments.persistence.sparql;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.profiling.FilterUserData;
import eu.fbk.utils.core.CommandLine;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads RDF data into memory and emulates a SPARQL endpoint we typically use
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class InMemoryEndpoint extends FakeEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterUserData.class);
    private static final String RDF_PATH = "rdf-path";
    private static final Pattern RDF_PATTERN = Pattern.compile("^<([^<>]+)>\\s+<([^<>]+)>\\s+((\"(.+)\"(((@[A-Za-z\\-]+)?)|((\\^\\^<([^<>]+)>)?)))|(<([^<>]+)>))\\s?\\.");

    public InMemoryEndpoint(File path) throws IOException {
        this(path, stream -> stream);
    }

    public InMemoryEndpoint(File path, Function<InputStream, InputStream> uncompressor) throws IOException {
        load(uncompressor.apply(new BufferedInputStream(new FileInputStream(path))));
    }

    public void load(InputStream rawRDF) throws IOException {
        HashMap<String, Map<String, List<String>>> resources = new HashMap<>();
        int accepted = 0;
        int skipped = 0;
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(rawRDF))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = RDF_PATTERN.matcher(line);
                if (!m.matches()) {
                    LOGGER.debug("Unexpected RDF format: "+line);
                    skipped++;
                    continue;
                }
                accepted++;
                if (accepted % 500000 == 0) {
                    info(String.format("Accepted RDF entries: %.1fm. Entities: %d",((float)accepted/1000000), resources.size()));
                }

                String object = m.group(1);
                String predicate = m.group(2);
                String subject = m.group(5);
                // Means subject is not a literal but a URI
                if (subject == null) {
                    subject = m.group(13);
                }

                Map<String, List<String>> resource;
                if (!resources.containsKey(object)) {
                    resource = new HashMap<>();
                    resources.put(object, resource);
                } else {
                    resource = resources.get(object);
                }
                List<String> values;
                if (!resource.containsKey(predicate)) {
                    values = new LinkedList<>();
                    resource.put(predicate, values);
                } else {
                    values = resource.get(predicate);
                }
                values.add(subject);
            }
        }
        info(String.format("Total accepted RDF entries: %.3fm. Entities: %d. Skipped: %d", ((float)accepted/1000000), resources.size(), skipped));
        info("Finalizing");
        for (Map.Entry<String, Map<String, List<String>>> rawResource : resources.entrySet()) {
            DBpediaResource resource = new DBpediaResource(rawResource.getKey(), rawResource.getValue());
            register(resource);
        }
        resources.clear();
        info("Done");
    }

    private void info(String message) {
        LOGGER.info("["+this.getClass().getSimpleName()+"] "+message);
    }

    public static InMemoryEndpoint uncompressAndLoad(File path) throws IOException {
        return new InMemoryEndpoint(
            path,
            stream -> {
                try {
                    return new CompressorStreamFactory().createCompressorInputStream(stream);
                } catch (CompressorException e) {
                    LOGGER.warn("Uncompressor failed, trying plain text: ", e);
                }
                return stream;
            }
        );
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("i", RDF_PATH,
                        "specifies the file for the RDF data", "FILE",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            //noinspection ConstantConditions
            final String rdfPath = cmd.getOptionValue(RDF_PATH, String.class);

            InMemoryEndpoint endpoint = uncompressAndLoad(new File(rdfPath));
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
