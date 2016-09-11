package eu.fbk.ict.fm.smt;

import eu.fbk.ict.fm.smt.services.AlignmentsService;
import eu.fbk.ict.fm.smt.util.CORSResponseFilter;
import eu.fbk.ict.fm.smt.util.ConnectionFactory;
import eu.fbk.ict.fm.smt.util.TwitterCredentials;
import eu.fbk.ict.fm.smt.util.TwitterFactory;
import org.apache.commons.cli.*;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import twitter4j.Twitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;

/**
 * Starting point for the web application
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Application {
    private static TwitterCredentials credentials;
    private static ConnectionFactory.Credentials alignments;

    public static void main(String[] args) throws URISyntaxException, FileNotFoundException {
        Configuration config = loadConfiguration(args);
        if (config == null) {
            return;
        }
        alignments = ConnectionFactory.getConf(config.connection);
        credentials = TwitterCredentials.credentialsFromFile(new File(config.credentials))[0];

        final ResourceConfig rc = new ResourceConfig().packages(Application.class.getPackage().getName());
        rc.register(new Binder());
        rc.register(new CORSResponseFilter());
        URI uri = new URI(null, null, "0.0.0.0", config.port, null, null, null);
        final HttpServer httpServer =  GrizzlyHttpServerFactory.createHttpServer(uri, rc);

        try {
            httpServer.start();
            Thread.currentThread().join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class Binder extends AbstractBinder {

        @Override
        protected void configure() {
            bind(credentials).to(TwitterCredentials.class);
            bind(alignments).to(ConnectionFactory.Credentials.class);
            bindFactory(TwitterFactory.class).to(Twitter.class);
            bindFactory(ConnectionFactory.class).to(Connection.class);
            bind(AlignmentsService.class).to(AlignmentsService.class);
        }
    }

    public static class Configuration {
        public Integer port;
        public String connection;
        public String credentials;
    }

    public static Configuration loadConfiguration(String[] args) {
        Options options = new Options();
        options.addOption(
                Option.builder("p").desc("Port")
                        .required().hasArg().argName("port").longOpt("port").build()
        );
        options.addOption(
                Option.builder("c").desc("Twitter credentials")
                        .required().hasArg().argName("file").longOpt("credentials").build()
        );
        options.addOption(
                Option.builder().desc("Serialized connection to the database")
                        .required().hasArg().argName("db").longOpt("db").build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine line;

        try {
            // parse the command line arguments
            line = parser.parse(options, args);

            Configuration config = new Configuration();
            config.port = Integer.valueOf(line.getOptionValue("port"));
            config.connection = line.getOptionValue("db");
            config.credentials = line.getOptionValue("credentials");
            return config;
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
                "java -Dfile.encoding=UTF-8 "+ConvertOldAlignmentsToNew.class.getName(),
                "\n",
                options,
                "\n",
                true
        );
    }
}
