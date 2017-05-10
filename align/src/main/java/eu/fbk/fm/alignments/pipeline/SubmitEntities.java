package eu.fbk.fm.alignments.pipeline;

import eu.fbk.fm.alignments.index.FillFromIndex;
import eu.fbk.fm.alignments.index.db.tables.records.AlignmentsRecord;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.core.CommandLine;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.fbk.fm.alignments.index.db.tables.Alignments.ALIGNMENTS;

/**
 * Read the list of entities, produce the list of candidates and save everything to the database
 */
public class SubmitEntities {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubmitEntities.class);

    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";
    private static final String ENDPOINT = "endpoint";
    private static final String INPUT = "input";

    private final DataSource source;
    private final FillFromIndex index;
    private final FillFromIndex fallbackIndex;


    public SubmitEntities(DataSource source, Endpoint endpoint) {
        this.source = source;
        this.index = new FillFromIndex(endpoint, new AllNamesStrategy(), source);
        this.index.setTimeout(20);
        this.index.turnOffVerbose();
        this.fallbackIndex = new FillFromIndex(endpoint, new AllNamesStrategy(1), source);
        this.fallbackIndex.setTimeout(20);
        this.fallbackIndex.turnOffVerbose();
    }

    public void run(String input) throws IOException {
        AtomicInteger counter = new AtomicInteger(0);
        Files.lines(Paths.get(input)).parallel().forEach(line -> {
            //Populating list of candidates
            line = line.substring(1, line.length()-1);
            List<Long> uids = index.getUids(line);
            if (uids.size() == 0) {
                uids = fallbackIndex.getUids(line);
            }

            //Saving everything to the database
            DSLContext context = DSL.using(source, SQLDialect.POSTGRES);
            List<AlignmentsRecord> records = new LinkedList<>();
            for (Long candidate : uids) {
                AlignmentsRecord record = context.newRecord(ALIGNMENTS);

                record.setIsAlignment(false);
                record.setVersion((short) 0);
                record.setUid(candidate);
                record.setResourceId(line);
                record.setScore(0.0f);

                records.add(record);
            }
            try {
                context.batchInsert(records).execute();
            } catch (Exception e) {
                LOGGER.error("Something terrible happened during the insert", e);
            }
            int processed = counter.incrementAndGet();
            if (processed % 10000 == 0) {
                LOGGER.info("Processed "+processed+" entities");
            }
        });
    }

    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("c", DB_CONNECTION,
                        "connection string for the database", "DB",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_USER,
                        "user for the database", "USER",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_PASSWORD,
                        "password for the database", "PASSWORD",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, ENDPOINT,
                        "URL to SPARQL endpoint", "ENDPOINT",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, INPUT,
                        "Input file with entities", "INPUT",
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String dbConnection = cmd.getOptionValue(DB_CONNECTION, String.class);
            final String dbUser = cmd.getOptionValue(DB_USER, String.class);
            final String dbPassword = cmd.getOptionValue(DB_PASSWORD, String.class);
            final String endpointUri = cmd.getOptionValue(ENDPOINT, String.class);
            final String input = cmd.getOptionValue(INPUT, String.class);

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);
            Endpoint endpoint = new Endpoint(endpointUri);
            SubmitEntities script = new SubmitEntities(source, endpoint);

            script.run(input);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
