package eu.fbk.ict.fm.smt.services;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import eu.fbk.utils.data.DatasetRepository;
import eu.fbk.utils.data.dataset.Dataset;
import eu.fbk.utils.data.dataset.bow.FeatureMapping;
import eu.fbk.utils.data.dataset.bow.FeatureMappingInterface;
import eu.fbk.utils.data.dataset.bow.NGramMapping;
import org.apache.commons.cli.*;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A service that makes requests to Redis to resolve ngrams
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service
@Singleton
public class NGramsService implements FeatureMappingInterface {
    private final static Logger logger = LoggerFactory.getLogger(eu.fbk.fm.alignments.persistence.NGramsService.class);
    private final static int BATCH_SIZE = 1000;

    private String endpointUri;
    private RedisClient client;
    private RedisAsyncCommands<String, String> commands;

    @Inject
    public NGramsService(@Named("NgramsEndpoint") String endpoint) {
        endpointUri = endpoint;
        client = RedisClient.create(endpoint);
        commands = client.connect().async();
    }

    public void close() {
        commands.close();
        client.shutdown();
    }

    public void restoreFromDataset() {
        //Downloading the resource
        Dataset dataset;
        try {
            dataset = new DatasetRepository().load("ngrams");
            if (!(dataset instanceof NGramMapping)) {
                throw new Exception("The instantiated dataset is of the wrong type");
            }
        } catch (Exception e) {
            logger.error("The database restoration process will halt due to an error", e);
            return;
        }

        //Mapping features and loading them into the Redis
        NGramMapping mapping = (NGramMapping) dataset;
        Map<String, String> batch = new HashMap<>();
        List<RedisFuture<String>> results = new LinkedList<>();
        int counter = 0;
        for (Map.Entry<String, FeatureMapping.Feature> ngram : mapping.getRawMap().entrySet()) {
            batch.put(ngram.getKey(), ngram.getValue().toString());
            if (batch.size() == BATCH_SIZE) {
                results.add(commands.mset(batch));
                batch.clear();
            }
            counter++;
            if (counter % 1000000 == 0) {
                logger.info("Processed "+(counter/1000000)+"m entities");
            }
        }
        results.add(commands.mset(batch));

        //Awaiting completion
        int requests = results.size();
        while (results.size() != 0) {
            Iterator<RedisFuture<String>> it = results.iterator();
            while (it.hasNext()) {
                RedisFuture<String> future = it.next();
                try {
                    if (future.isDone() || future.isCancelled() || future.await(30, TimeUnit.SECONDS)) {
                        it.remove();
                    }
                } catch (InterruptedException e) {
                    //ignore
                }
            }
            logger.info(String.format("%d out of %d requests alive", results.size(), requests));
        }

        try {
            logger.info("Final DB size: "+commands.dbsize().get());
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Can't compute DB size: "+e.getMessage());
        }
    }

    public FeatureMapping.Feature lookup(String ngram) {
        try {
            return FeatureMapping.Feature.fromString(commands.get(ngram).get());
        } catch (InterruptedException | ExecutionException e) {
            return null;
        }
    }

    public List<FeatureMapping.Feature> lookup(List<String> ngrams) {
        try {
            return commands
                    .mget(ngrams.toArray(new String[0]))
                    .get()
                    .stream()
                    .map(FeatureMapping.Feature::fromString)
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            return new LinkedList<>();
        }
    }

    public static void main(String[] args) throws URISyntaxException, FileNotFoundException {
        Configuration config = loadConfiguration(args);
        if (config == null) {
            return;
        }

        eu.fbk.fm.alignments.persistence.NGramsService service = new eu.fbk.fm.alignments.persistence.NGramsService(config.endpoint);
        service.restoreFromDataset();
        service.close();
    }

    public static class Configuration {
        public String endpoint;
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("e").desc("Redis endpoint URI")
                        .required().hasArg().argName("uri").longOpt("endpoint").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            Configuration config = new Configuration();
            config.endpoint = line.getOptionValue("endpoint");
            return config;
        } catch (ParseException exp) {
            // oops, something went wrong
            System.out.println(String.join(", ", args));
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
                "java -Dfile.encoding=UTF-8 "+ eu.fbk.fm.alignments.persistence.NGramsService.class.getName(),
                "\n",
                options,
                "\n",
                true
        );
    }
}
