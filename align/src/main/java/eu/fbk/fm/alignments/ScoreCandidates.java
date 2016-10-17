package eu.fbk.fm.alignments;

import org.apache.log4j.Logger;

/**
 * Aligned an entity with a single most probable candidate
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ScoreCandidates {
    private static final Logger logger = Logger.getLogger(ScoreCandidates.class.getName());
    private static final int THREADS = 50;
    private static final int LIMIT = 10000;
    private static final String BATCH_QUERY = "" +
            "SELECT * FROM `alignments` " +
            "WHERE `source` = :source AND `candidates` IS NOT NULL " +
            "LIMIT 0, :limit";
    public static final String SELECT_CANDIDATES = "" +
            "SELECT * FROM `users` " +
            "WHERE `twitter_id` IN (:candidates)";

    /*private SessionFactory sf;
    private Endpoint sparqlEndpoint;

    private Iterator iterator;
    private int processed = 0;
    private int lastBatchQuerySize = 0;

    public ScoreCandidates(SessionFactory sf, Endpoint sparqlEndpoint) {
        this.sf = sf;
        this.sparqlEndpoint = sparqlEndpoint;
    }

    public void run(String workdir) throws Exception {
        FileProvider fileProvider = new FileProvider(workdir);
        Worker[] workers = new Worker[THREADS];
        DefaultScoringStrategy strategy = new DefaultScoringStrategy();
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(fileProvider, sf, sparqlEndpoint, strategy);
        }
        int counter = 0, alive;
        populateBatch();
        while (lastBatchQuerySize > 0) {
            counter++;
            alive = 0;
            for (Worker worker : workers) {
                if (worker.isAlive()) {
                    alive++;
                    continue;
                }

                AlignmentInfo info = getNextItem();
                if (info != null) {
                    worker.start(info);
                    alive++;
                }
            }

            if (alive == 0) {
                printStats(workers);
                populateBatch();
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (counter % 20 == 0) {
                logger.info(String.format("Processed %d items", processed));
            }
        }
        logger.info("Done");
        printStats(workers);
    }

    public void printStats(Worker[] workers) {
        int[] persons = new int[]{0, 0, 0};
        int[] organisations = new int[]{0, 0, 0};
        double candidatesPersons = 0;
        double candidatesOrganisations = 0;
        for (Worker worker : workers) {
            int[] locPersons = worker.getPersons();
            int[] locOrganisations = worker.getOrganisations();
            for (int i = 0; i < persons.length; i++) {
                persons[i] += locPersons[i];
            }
            for (int i = 0; i < organisations.length; i++) {
                organisations[i] += locOrganisations[i];
            }
            candidatesPersons += worker.getCandidatesPersons();
            candidatesOrganisations += worker.getCandidatesOrganisations();
        }
        candidatesPersons = candidatesPersons / persons[1];
        candidatesOrganisations = candidatesOrganisations / organisations[1];
        logger.info(String.format("Persons. Total: %d, with candidate: %d, aligned: %d, avg. cand: %.2f",
                persons[0], persons[1], persons[2], candidatesPersons));
        logger.info(String.format("Orgs.    Total: %d, with candidate: %d, aligned: %d, avg. cand: %.2f",
                organisations[0], organisations[1], organisations[2], candidatesOrganisations));
    }

    private AlignmentInfo getNextItem() {
        if (iterator == null || !iterator.hasNext()) {
            return null;
        }
        processed++;
        return (AlignmentInfo) iterator.next();
    }

    private void populateBatch() {
        logger.info("Querying new batch");
        Session session = Utf8Fix.openSession(sf);
        SQLQuery query = session.createSQLQuery(BATCH_QUERY);
        query.setString("source", AlignMissing.TASK_DB_SOURCE);
        query.setInteger("limit", LIMIT);
        query.setCacheable(false);
        query.addEntity(AlignmentInfo.class);

        List result = query.list();
        lastBatchQuerySize = result.size();
        iterator = result.iterator();
        session.close();
    }

    private static class Worker implements Runnable {
        private int missed;
        private int[] persons = new int[]{0, 0, 0};
        private int[] organisations = new int[]{0, 0, 0};
        private double candidatesPersons = 0;
        private double candidatesOrganisations = 0;
        private SessionFactory sf;
        private Endpoint sparqlEndpoint;
        private Scaler scaler;
        //private SVMPredictor predictor;
        private ModelEndpoint modelEndpoint;
        private ScoringStrategy strategy;

        private Thread thread = null;
        private AlignmentInfo info = null;

        public Worker(FileProvider provider, SessionFactory sf, Endpoint sparqlEndpoint, ScoringStrategy strategy) throws IOException, URISyntaxException {
            this.sf = sf;
            this.scaler = new Gson().fromJson(new FileReader(provider.scaler), Scaler.class);
            //this.predictor = new SVMPredictor(provider.model.getAbsolutePath());
            this.sparqlEndpoint = sparqlEndpoint;
            this.modelEndpoint = new ModelEndpoint();
            this.strategy = strategy;
        }

        public void start(AlignmentInfo info) {
            thread = new Thread(this);
            this.info = info;
            thread.start();
        }

        public boolean isAlive() {
            return thread != null && thread.isAlive();
        }

        public void run(Session session) throws Exception {
            DBpediaResource resource = sparqlEndpoint.getResourceById(info.getResourceId());
            if (resource.getAttributes().size() == 0) {
                logger.warn("Resource " + resource.getIdentifier() + " doesn't contain any properties");
                info.setSource("Empty");
                return;
            }
            if (resource.isPerson()) {
                persons[0]++;
            } else if (resource.isCompany()) {
                organisations[0]++;
            }
            if (info.getCandidates().length == 0) {
                missed++;
                if (missed % 100 == 0) {
                    logger.warn("No candidates for " + missed + " entities. Latest: " + info.getResourceId());
                }
                return;
            }
            if (resource.isPerson()) {
                persons[1]++;
                candidatesPersons += info.getCandidates().length;
            } else if (resource.isCompany()) {
                organisations[1]++;
                candidatesOrganisations += info.getCandidates().length;
            }
            Gson gson = TwitterDeserializer.getDefault().getBuilder().create();
            FullyResolvedEntry entry = new FullyResolvedEntry(resource);
            try {
                SQLQuery query = session.createSQLQuery(SELECT_CANDIDATES);
                Map<Long, Integer> candidates = new HashMap<>();
                AlignmentInfo.Candidate[] rawCandidates = info.getCandidates();
                if (rawCandidates.length == 0) {
                    return;
                }
                for (AlignmentInfo.Candidate candidate : rawCandidates) {
                    candidates.put(candidate.id, candidate.order);
                }
                entry.candidates = new ArrayList<>(candidates.size());
                for (int i = 0; i < candidates.size(); i++) {
                    entry.candidates.add(null);
                }
                query.setParameterList("candidates", candidates.keySet());
                query.setCacheable(false);
                query.addEntity(UserObject.class);
                List queryResult = query.list();
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format(
                            "Selected %d candidates for resource %s",
                            queryResult.size(),
                            info.getResourceId()
                    ));
                }
                for (Object obj : queryResult) {
                    UserObject userObj = (UserObject) obj;
                    int index = candidates.getOrDefault(userObj.getTwitterId(), -1);
                    if (index == -1) {
                        throw new Exception("Can't find a user with this ID in the candidate list");
                    }
                    entry.candidates.set(index, gson.fromJson(userObj.getObject(), User.class));
                }
                String[] screenNames = new String[entry.candidates.size()];
                int order = 0;
                for (User user : entry.candidates) {
                    screenNames[order] = user.getScreenName();
                    order++;
                }
                info.setCandidateUsernames(gson.toJson(screenNames));
                strategy.fillScore(entry);

                order = 0;
                int maxOrder = -1;
                double maxScore = -1.0;
                double nextScore = -1.0;
                double[] scores = new double[entry.features.size()];
                for (double[] features : entry.features) {
                    scaler.transform(features);
                    double[] prediction = modelEndpoint.predict(features);
                    //svm_node[] svmFeatures = predictor.nodes(features);
                    //boolean isPositive = (int) predictor.predict(svmFeatures) == 1;
                    //boolean isPositive = prediction[1] > 0.5;
                    double score = prediction[1];
                    scores[order] = score;
                    //if (isPositive) {
                    //double score = Math.abs(predictor.predictBinaryWithScore(svmFeatures));
                    if (score > nextScore) {
                        if (score > maxScore) {
                            maxOrder = order;
                            nextScore = maxScore;
                            maxScore = score;
                        } else {
                            nextScore = score;
                        }
                    }
                    //}
                    order++;
                }
                info.setScores(gson.toJson(scores));
                info.setTwitterId(0);
                //if (maxScore >= 0.5 && (maxScore - nextScore) >= 0.5) {
                if (maxScore >= 0.0) {
                    info.setTwitterId(entry.candidates.get(maxOrder).getId());
                    if (resource.isPerson()) {
                        persons[2]++;
                    } else if (resource.isCompany()) {
                        organisations[2]++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                Bug.report(e);
                logger.error(e);
            }
        }

        @Override
        public void run() {
            try {
                Session session = Utf8Fix.openSession(sf);
                info.setSource(ScoreCandidates.class.getSimpleName());
                run(session);
                session.beginTransaction();
                session.update(info);
                session.getTransaction().commit();
                session.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public int[] getPersons() {
            return persons;
        }

        public int[] getOrganisations() {
            return organisations;
        }

        public double getCandidatesPersons() {
            return candidatesPersons;
        }

        public double getCandidatesOrganisations() {
            return candidatesOrganisations;
        }
    }

    public static void main(String[] args) throws Exception {
        LoggerConfigurator.def();
        Configuration configuration = ScoreCandidates.loadConfiguration(args);
        Endpoint endpoint = new Endpoint(configuration.getString("endpoint"));
        SessionFactory sf = new DBProvider().getFactory(configuration.getString("password"));
        ScoreCandidates scorer = new ScoreCandidates(sf, endpoint);
        scorer.run(configuration.getString("workdir"));
        System.exit(0);
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("e").desc("Url to SPARQL endpoint")
                        .required().hasArg().argName("file").longOpt("endpoint").build()
        );
        options.addOption(
                Option.builder("p").desc("Database password")
                        .required().hasArg().argName("pass").longOpt("password").build()
        );
        options.addOption(
                Option.builder("w").desc("Working directory containing trained model and scaler")
                        .required().hasArg().argName("workdir").longOpt("workdir").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            Configuration configuration = new PropertiesConfiguration();
            configuration.setProperty("endpoint", line.getOptionValue("endpoint"));
            configuration.setProperty("password", line.getOptionValue("password"));
            configuration.setProperty("workdir", line.getOptionValue("workdir"));
            return configuration;
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
                "java -Dfile.encoding=UTF-8 eu.fbk.ict.fm.profiling.converters.eu.fbk.fm.alignments.ScoreCandidates",
                "\n",
                options,
                "\n",
                true
        );
    }*/
}
