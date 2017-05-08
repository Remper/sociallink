package eu.fbk.fm.alignments.index;

import eu.fbk.fm.alignments.Evaluate;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import eu.fbk.fm.alignments.query.StrictStrategy;
import eu.fbk.fm.alignments.query.index.AllNamesStrategy;
import eu.fbk.fm.alignments.scorer.DBTextScorer;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.utils.DBUtils;
import eu.fbk.utils.core.CommandLine;
import eu.fbk.utils.lsa.LSM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;

/**
 * Simple tool to test LSA index
 */
public class UserLSAInteractive {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserLSAInteractive.class);

    private static final String DB_CONNECTION = "db-connection";
    private static final String DB_USER = "db-user";
    private static final String DB_PASSWORD = "db-password";
    private static final String ENDPOINT = "endpoint";
    private static final String LSA_PATH = "lsa-path";
    private static final String QUERY = "query";

    protected LSM lsm;
    protected FillFromIndex index;
    protected DataSource source;

    public UserLSAInteractive(Endpoint endpoint, DataSource source, LSM lsm) {
        this.lsm = lsm;
        this.index = new FillFromIndex(endpoint, new AllNamesStrategy(), source);
        this.source = source;
    }

    public void run(String query) {
        FullyResolvedEntry entry = new FullyResolvedEntry(new Evaluate.DatasetEntry(query, null));

        index.fill(entry);

        LOGGER.info("List of candidates for entity: "+entry.entry.resourceId);
        DBTextScorer scorer = new DBTextScorer(source, new LSAVectorProvider(lsm));
        for (User candidate : entry.candidates) {
            LOGGER.info("  "+candidate.getName()+" (@"+candidate.getScreenName()+")");
            LOGGER.info("     Similarity: "+scorer.getFeature(candidate, entry.resource));
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
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, QUERY,
                        "Query", "QUERY",
                        CommandLine.Type.STRING, true, false, false);
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
            String query = cmd.getOptionValue(QUERY, String.class);
            if (query == null) {
                query = "http://wikidata.dbpedia.org/resource/Q359442";
            }

            DataSource source = DBUtils.createPGDataSource(dbConnection, dbUser, dbPassword);
            LSM lsm = new LSM(lsaPath + "/X", 100, true);
            UserLSAInteractive script = new UserLSAInteractive(new Endpoint(endpointUri), source, lsm);

            script.run(query);
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
        }
    }
}
