package eu.fbk.fm.alignments.rdf;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Charsets;

import org.apache.flink.hadoop.shaded.com.google.common.collect.Ordering;
import org.apache.flink.shaded.com.google.common.base.Preconditions;
import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.utils.core.CommandLine;

// NOTE
// ====
// the emitted dataset X has to be postprocessed with this command to obtain the final dataset Y
// rdfpro @read X @kvread -m s sameas.kv @unique @write Y
// where sameas.kv identifies two files in the current directory with an index of key/value
// statements (unzip sameas.kv.zip under ganymede1/data/dbpedia)

public class RDFExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFExporter.class);

    public static void main(final String... args) {

        try {
            // Define and parse command line options
            final CommandLine cmd = CommandLine.parser().withName("rdf-exporter")
                    .withHeader("Export the alignments in the database to an RDF file")
                    .withOption("d", "db", "the database JDBC URL", "URL", CommandLine.Type.STRING,
                            true, false, true)
                    .withOption("u", "username", "the database username", "STRING",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("p", "password", "the database password", "STRING",
                            CommandLine.Type.STRING, true, false, false)
                    .withOption("o", "output", "the RDF file to emit", "PATH",
                            CommandLine.Type.FILE, true, false, true)
                    .withLogger(LoggerFactory.getLogger("eu.fbk")).parse(args);

            // Read options
            final String url = cmd.getOptionValue("d", String.class);
            final String username = cmd.getOptionValue("u", String.class, "");
            final String password = cmd.getOptionValue("p", String.class, "");
            final Path path = cmd.getOptionValue("o", Path.class);

            // Acquire a connection to the DB and export the data
            Class.forName("org.postgresql.Driver");
            try (Connection connection = DriverManager.getConnection(url, username, password)) {
                export(connection, path);
            }

        } catch (final Throwable ex) {
            // Abort execution, returning appropriate error code
            CommandLine.fail(ex);
        }

    }

    private static Map<Long, String> loadScreenNames(final Connection connection, final Path path)
            throws SQLException, IOException {

        final Map<Long, String> map = Maps.newHashMap();

        final Path cachePath = Paths.get(path.toString() + ".cache");
        if (Files.exists(cachePath)) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(Files.newInputStream(cachePath)), Charsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    final String[] fields = line.split("\t");
                    map.put(Long.valueOf(fields[0]), fields[1]);
                }
            }
            LOGGER.info("{} screen names loaded from {}", map.size(), cachePath);
            return map;
        }

        connection.setAutoCommit(false);
        try (PreparedStatement stmt = connection
                .prepareStatement("SELECT   objects.uid, objects.object::TEXT AS json "
                        + "FROM   (SELECT user_objects.* FROM user_objects WHERE uid IN (SELECT DISTINCT uid FROM alignments)) AS objects")) {
            stmt.setFetchSize(1000);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    final long uid = rs.getLong(1);
                    final String json = rs.getString(2);
                    final String key = "\"screen_name\":\"";
                    final int start = json.indexOf(key) + key.length();
                    final int end = json.indexOf('"', start);
                    final String screenName = json.substring(start, end);
                    map.put(uid, screenName);
                    if (map.size() % 1000 == 0) {
                        LOGGER.info("{} screen names fetched from DB", map.size());
                    }
                }
            }
        }

        try (Writer out = new OutputStreamWriter(
                new GZIPOutputStream(Files.newOutputStream(cachePath)), Charsets.UTF_8)) {
            for (final Long id : Ordering.natural().sortedCopy(map.keySet())) {
                out.write(id.toString());
                out.write("\t");
                out.write(map.get(id));
                out.write("\n");
            }
            LOGGER.info("{} screen names stored to {}", map.size(), cachePath);
        }

        return map;
    }

    public static void export(final Connection connection, final Path path)
            throws SQLException, IOException, RDFHandlerException {

        final Map<Long, String> screenNames = loadScreenNames(connection, path);

        connection.setAutoCommit(false);

        final ValueFactory vf = SimpleValueFactory.getInstance();

        try (Writer writer = new OutputStreamWriter(
                new GZIPOutputStream(new FileOutputStream(path.toFile())), Charsets.UTF_8)) {

            final RDFWriter out = Rio.createWriter(RDFFormat.NTRIPLES, writer);
            out.startRDF();

            int counter = 0;
            try (PreparedStatement stmt = connection.prepareStatement("" //
                    + "SELECT resource_id, uid, score, is_alignment\n" //
                    + "FROM alignments\n" //
                    + "WHERE version=2\n" //
                    + "ORDER BY resource_id ASC, score DESC")) {
                stmt.setFetchSize(1000);
                try (ResultSet rs = stmt.executeQuery()) {
                    IRI lastEntity = null;
                    int rank = 1;
                    while (rs.next()) {
                        final long uid = rs.getLong("uid");
                        final String screenName = screenNames.get(uid);
                        Preconditions.checkState(screenName != null);
                        final IRI entity = vf.createIRI(rs.getString("resource_id"));
                        final IRI account = vf.createIRI(
                                "http://twitter.com/" + screenName.replaceAll("\\s", "+"));
                        final BNode candidate = vf.createBNode();
                        final float score = rs.getFloat("score");
                        final boolean align = rs.getBoolean("is_alignment");
                        rank = entity.equals(lastEntity) ? rank + 1 : 1;
                        lastEntity = entity;
                        LOGGER.debug("Processing candidate {} {} {} {}", entity, candidate, score,
                                align);
                        if (align) {
                            emit(out, entity, FOAF.ACCOUNT, account);
                        }
                        emit(out, entity, OWL.SAMEAS, entity);
                        emit(out, entity, SL.CANDIDATE_PROPERTY, candidate);
                        emit(out, candidate, SL.RANK, vf.createLiteral(rank));
                        emit(out, candidate, SL.CONFIDENCE, vf.createLiteral(score));
                        emit(out, candidate, SL.ACCOUNT, account);
                        emit(out, account, DCTERMS.IDENTIFIER, vf.createLiteral(uid));
                        emit(out, account, FOAF.ACCOUNT_NAME, vf.createLiteral(screenName));
                        if (++counter % 10000 == 0) {
                            LOGGER.info("{} candidates processed", counter);
                        }
                    }
                }
            }

            out.endRDF();
        }
    }

    private static void emit(final RDFHandler out, final Resource s, final IRI p, final Value o)
            throws RDFHandlerException {
        final ValueFactory vf = SimpleValueFactory.getInstance();
        out.handleStatement(vf.createStatement(s, p, o));
    }

}
