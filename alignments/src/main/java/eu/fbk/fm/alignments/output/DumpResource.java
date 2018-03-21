package eu.fbk.fm.alignments.output;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.core.CommandLine;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Queries the database and dumps resource on disk
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class DumpResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DumpResource .class);
    private static final Gson gson = new Gson();

    private static final String PRETTY_JSON = "sociallink.pretty.json.gz";
    private static final String JSON = "sociallink.json.gz";
    private static final String CSV = "sociallink.csv.gz";

    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";

    private static final String QUERY = "" +
            "SELECT resource_id, json_agg((uid, score, is_alignment)) FROM alignments\n" +
            "WHERE version = 2\n" +
            "GROUP BY resource_id";

    private final DataSource source;

    public DumpResource(DataSource source) {
        this.source = source;
    }

    public void run() throws IOException {
        ResultWriter[] writers = {
            new JSONResultWriter(new File(JSON)),
            new PrettyJSONResultWriter(new File(PRETTY_JSON)),
            new CSVResultWriter(new File(CSV))
        };

        AtomicInteger counter = new AtomicInteger();
        DSL.using(source, SQLDialect.POSTGRES_9_5)
            .fetchStream(QUERY).parallel().forEach(new Consumer<Record>() {
                @Override
                public void accept(Record record) {
                    List<Object> results = record.intoList();
                    String resourceId = results.get(0).toString();
                    Long trueUid = null;
                    JsonArray rawCandidates = gson.fromJson(results.get(1).toString(), JsonArray.class);
                    List<Candidate> candidates = new LinkedList<>();
                    for (JsonElement candidate : rawCandidates) {
                        Candidate parsedCandidate = Candidate.fromJSON(candidate.getAsJsonObject());
                        if (parsedCandidate.is_alignment) {
                            trueUid = parsedCandidate.uid;
                        }
                        candidates.add(parsedCandidate);
                    }

                    synchronized (this) {
                        for (ResultWriter writer : writers) {
                            writer.write(resourceId, candidates, trueUid);
                        }
                    }

                    int curValue = counter.incrementAndGet();
                    if (curValue % 1000 == 0) {
                        LOGGER.info("Processed "+curValue+" entities");
                    }
                }
            });

        for (ResultWriter writer : writers) {
            writer.close();
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
                        CommandLine.Type.STRING, true, false, true);
    }

    public static void main(String[] args) throws Exception {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            final String dbConnection = cmd.getOptionValue(DB_CONNECTION, String.class);
            final String dbUser = cmd.getOptionValue(DB_USER, String.class);
            final String dbPassword = cmd.getOptionValue(DB_PASSWORD, String.class);

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);
            DumpResource script = new DumpResource(source);

            script.run();
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }

    public static final class Candidate {
        long uid;
        float score;
        boolean is_alignment;

        public static Candidate fromJSON(JsonObject object) {
            Candidate candidate = new Candidate();
            candidate.uid = object.get("f1").getAsLong();
            candidate.score = object.get("f2").getAsFloat();
            candidate.is_alignment = object.get("f3").getAsBoolean();
            return candidate;
        }
    }
}
