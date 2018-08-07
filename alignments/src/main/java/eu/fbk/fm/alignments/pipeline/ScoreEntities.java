package eu.fbk.fm.alignments.pipeline;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.FileProvider;
import eu.fbk.fm.alignments.index.db.tables.records.AlignmentsRecord;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.scorer.ISWC17Strategy;
import eu.fbk.fm.alignments.scorer.ScoringStrategy;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.math.Scaler;
import org.jooq.Cursor;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.fbk.fm.alignments.index.db.tables.Alignments.ALIGNMENTS;
import static eu.fbk.fm.alignments.index.db.tables.UserObjects.USER_OBJECTS;

/**
 * Score candidates in the database
 */
public class ScoreEntities {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScoreEntities.class);

    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";
    private static final String ENDPOINT = "endpoint";
    private static final String LSA_PATH = "lsa-path";

    private final DataSource source;
    private final Endpoint endpoint;
    private final ModelEndpoint modelEndpoint;
    private final ScoringStrategy strategy;

    public ScoreEntities(DataSource source, Endpoint endpoint, ScoringStrategy strategy) throws URISyntaxException {
        this.source = source;
        this.endpoint = endpoint;
        this.modelEndpoint = new ModelEndpoint();
        this.strategy = strategy;
    }

    public void run() throws SQLException {
        AtomicInteger processed = new AtomicInteger(0);
        Stopwatch watch = Stopwatch.createStarted();
        boolean started = false;

        List<AlignmentsRecord> batch = new LinkedList<>();
        final ScoreEntities script = this;

        while (batch.size() > 0 || !started) {
            started = true;
            batch.clear();
            DSL.using(source, SQLDialect.POSTGRES)
                    .select(ALIGNMENTS.fields())
                    .select(USER_OBJECTS.OBJECT)
                    .from(ALIGNMENTS)
                    .leftJoin(USER_OBJECTS)
                    .on(USER_OBJECTS.UID.eq(ALIGNMENTS.UID))
                    .where(ALIGNMENTS.VERSION.eq((short) 0))
                    .limit(10000)
                    .fetch()
                    .parallelStream()
                    .forEach((record) -> {
                        // Parsing the result of the query
                        AlignmentsRecord alignment = new AlignmentsRecord(
                                record.get(ALIGNMENTS.RESOURCE_ID),
                                record.get(ALIGNMENTS.UID),
                                record.get(ALIGNMENTS.SCORE),
                                record.get(ALIGNMENTS.IS_ALIGNMENT),
                                (short) 1);

                        // Adding to a batch for update
                        synchronized (script) {
                            batch.add(alignment);
                        }

                        // Exiting if we are unhappy with the user object
                        Object userObj = record.get(USER_OBJECTS.OBJECT);
                        if (userObj == null) {
                            return;
                        }

                        User user;
                        try {
                            user = TwitterObjectFactory.createUser(userObj.toString());
                        } catch (TwitterException e) {
                            LOGGER.error("Error while deserializing user", e);
                            return;
                        }

                        // Getting entity from the KB
                        DBpediaResource resource = endpoint.getResourceById(alignment.getResourceId());

                        // Scoring and rescaling
                        Map<String, double[]> features = strategy.getScore(user, resource);

                        // Classifying
                        double result = modelEndpoint.predict(features)[1];

                        alignment.setScore((float) result);
                        alignment.setVersion((short) 2);

                        int curProcessed = processed.incrementAndGet();
                        if (curProcessed % 10000 == 0) {
                            LOGGER.info(String.format(
                                    "Processed %d entities (%.2f ent/s)",
                                    curProcessed,
                                    (double) 10000 / watch.elapsed(TimeUnit.SECONDS)));
                            watch.reset().start();
                        }
                    });

            DSL.using(source, SQLDialect.POSTGRES).batchUpdate(batch).execute();
        }
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
                .withOption(null, LSA_PATH,
                        "path to LSA model", "DIRECTORY",
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
            final String lsaPath = cmd.getOptionValue(LSA_PATH, String.class);

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);
            Endpoint endpoint = new Endpoint(endpointUri);
            ScoringStrategy strategy = ISWC17Strategy.builder().source(source).lsaPath(lsaPath).build();
            ScoreEntities script = new ScoreEntities(source, endpoint, strategy);

            script.run();
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
