package eu.fbk.fm.alignments;

import com.google.gson.Gson;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import twitter4j.TwitterException;
import twitter4j.User;

import java.io.File;
import java.math.BigInteger;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Try to align entities using Twitter API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class AlignMissing {
    private static final Logger logger = Logger.getLogger(AlignMissing.class.getName());
    private static final int LIMIT = 180;
    /*public static final String UNALIGNED_QUERY = "" +
            "SELECT * FROM `alignments` " +
            "WHERE `source` = :source " +
            "LIMIT 0, :limit";
    private static final String SECOND_TRY_QUERY = "" +
            "SELECT * FROM `alignments` " +
            "WHERE `candidates` IS NULL " +
            "LIMIT 0, :limit";
    private static final String TASK_STATS_QUERY = "" +
            "SELECT count(*) FROM twitter_dataset.alignments WHERE source = :unprocessed " +
            "UNION " +
            "SELECT count(*) FROM twitter_dataset.alignments WHERE source = :done";

    public static final String TASK_DB_SOURCE = AlignMissing.class.getSimpleName();

    private SessionFactory sf;
    private Endpoint endpoint;
    private QueryAssemblyStrategy qaStrategy = new StrictStrategy();

    public AlignMissing(Endpoint endpoint, SessionFactory sf) throws TwitterException {
        this.endpoint = endpoint;
        this.sf = sf;
    }

    public void setQAStrategy(QueryAssemblyStrategy qaStrategy) {
        this.qaStrategy = qaStrategy;
    }

    public void run(TwitterCredentials[] credentials) {
        SearchRunner.ResultReceiver receiver = new SearchRunner.ResultReceiver() {
            private int processed = 0;
            private final Gson gson = new Gson();

            @Override
            public synchronized void processResult(List<User> candidates, DBpediaResource task) {
                if (processed < 10) {
                    logger.info("Query: " + qaStrategy.getQuery(task) + ". Candidates: " + candidates.size());
                    processed++;
                }
                Session session = Utf8Fix.openSession(sf);
                session.beginTransaction();
                AlignmentInfo info = AlignmentInfo.create(task.getIdentifier(), 0, TASK_DB_SOURCE);
                info.setNames(gson.toJson(task.getNames()));

                long[] candidateIds = new long[candidates.size()];
                String[] candidateNames = new String[candidates.size()];
                int order = 0;
                for (User candidate : candidates) {
                    session.save(UserObject.create(new Date(), candidate));
                    candidateIds[order] = candidate.getId();
                    candidateNames[order] = candidate.getScreenName();
                    order++;
                }
                info.setCandidates(gson.toJson(candidateIds));
                info.setCandidateUsernames(gson.toJson(candidateNames));
                session.saveOrUpdate(info);
                session.flush();
                session.getTransaction().commit();
                session.close();
            }
        };
        SearchRunner[] runners;
        try {
            runners = SearchRunner.generateRunners(credentials, qaStrategy, receiver);
        } catch (TwitterException e) {
            logger.error("Can't instantiate runners", e);
            return;
        }

        SearchRunner.BatchProvider provider = new SearchRunner.BatchProvider() {
            private final String SOURCE = AddEntitiesToDatabase.DEFAULT_SOURCE;

            @Override
            public List<DBpediaResource> provideNextBatch() {
                logger.info("Computing new batch");
                int limit = runners.length * LIMIT;

                Session session = Utf8Fix.openSession(sf);
                SQLQuery query = session.createSQLQuery(UNALIGNED_QUERY);
                query.setInteger("limit", limit);
                query.setString("source", SOURCE);
                query.setCacheable(false);
                query.addEntity(AlignmentInfo.class);

                List<DBpediaResource> results = new LinkedList<>();
                for (Object result : query.list()) {
                    results.add(endpoint.getResourceById(((AlignmentInfo) result).getResourceId()));
                }
                session.close();
                return results;
            }

            @Override
            public void printStats(Date startDate, int processed) {
                Session session = Utf8Fix.openSession(sf);
                SQLQuery query = session.createSQLQuery(TASK_STATS_QUERY);
                query.setString("unprocessed", SOURCE);
                query.setString("done", TASK_DB_SOURCE);
                query.setCacheable(false);
                List results = query.list();
                int done = ((BigInteger) results.get(1)).intValue();
                int togo = ((BigInteger) results.get(0)).intValue();
                logger.info(String.format("Done %d (%.2f%%). %d to go", done, ((float) done / (done + togo)) * 100, togo));
                if (processed > 0) {
                    long curTime = new Date().getTime();
                    double speed = ((float) (curTime - startDate.getTime())) / processed;
                    Date target = new Date(curTime + (int) (speed * togo));
                    logger.info("Projection: " + timeLeft(target) + " left (" + target.toString() + ")");
                }
                session.close();
            }
        };
        SearchRunner.startRun(runners, provider);
    }

    private String timeLeft(Date target) {
        long ms = target.getTime() - new Date().getTime();
        if (ms < 1000) {
            return ms + "ms";
        }
        ms = ms / 1000;
        if (ms < 60) {
            return ms + "s";
        }
        ms = ms / 60;
        if (ms < 60) {
            return ms + "m";
        }
        ms = ms / 60;
        if (ms < 24) {
            return ms + "h";
        }
        ms = ms / 24;
        return ms + "d";
    }

    public static void main(String[] args) throws Exception {
        LoggerConfigurator.def();
        Configuration configuration = AlignMissing.loadConfiguration(args);
        SessionFactory sf = new DBProvider().getFactory(configuration.getString("password"));
        Endpoint endpoint = new Endpoint(configuration.getString("endpoint"));
        AlignMissing missing = new AlignMissing(endpoint, sf);
        missing.run(TwitterCredentials.credentialsFromFile(new File(configuration.getString("credentials"))));
        try {
            sf.close();
        } catch (HibernateException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("e").desc("Url to SPARQL endpoint")
                        .required().hasArg().argName("file").longOpt("endpoint").build()
        );
        options.addOption(
                Option.builder("c").desc("File from which to read Twitter credendtials")
                        .required().hasArg().argName("file").longOpt("credentials").build()
        );
        options.addOption(
                Option.builder("p").desc("Database password")
                        .required().hasArg().argName("pass").longOpt("password").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            File configFile;
            if (line.hasOption("config")) {
                configFile = new File(line.getOptionValue("config"));
            } else {
                configFile = new File(DefaultConfiguration.DEFAULT_CONFIG_FILE_NAME);
            }
            try {
                logger.info("Reading configuration from " + configFile.getAbsolutePath() + "...");
                Configuration configuration = new PropertiesConfiguration(configFile);
                configuration.setProperty("endpoint", line.getOptionValue("endpoint"));
                configuration.setProperty("credentials", line.getOptionValue("credentials"));
                configuration.setProperty("password", line.getOptionValue("password"));
                return configuration;
            } catch (ConfigurationException e) {
                logger.error(e);
                return null;
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed: " + exp.getMessage() + "\n");
            printHelp(options);
            System.exit(1);
        }
        return null;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                200,
                "java -Dfile.encoding=UTF-8 eu.fbk.ict.fm.profiling.converters.eu.fbk.fm.alignments.AlignMissing",
                "\n",
                options,
                "\n",
                true
        );
    }*/
}
