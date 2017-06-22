package eu.fbk.ict.fm.smt;

import com.google.gson.Gson;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.ict.fm.smt.filters.EncodingResponseFilter;
import eu.fbk.ict.fm.smt.services.*;
import eu.fbk.ict.fm.smt.filters.CORSResponseFilter;
import eu.fbk.ict.fm.smt.util.ConnectionFactory;
import eu.fbk.ict.fm.smt.util.TwitterCredentials;
import eu.fbk.ict.fm.smt.util.TwitterFactory;
import eu.fbk.utils.math.Scaler;
import org.apache.commons.cli.*;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.ConnectionProvider;
import twitter4j.Twitter;

import javax.inject.Singleton;
import javax.sql.DataSource;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Starting point of the web application
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Application {
    private static TwitterCredentials[] credentials;
    private static DataSource alignments;
    private static String sparqlLocation;
    private static String wikimachineEndpoint;
    private static String ngramsEndpoint;
    private static String lsaFilename;
    private static Scaler scaler;
    private static ModelEndpoint modelEndpoint;

    public static void main(String[] args) throws URISyntaxException, FileNotFoundException {
        Configuration config = loadConfiguration(args);
        if (config == null) {
            return;
        }
        Gson gson = new Gson();
        ngramsEndpoint = config.ngramsEndpoint;
        wikimachineEndpoint = config.wikimachineEndpoint;
        alignments = ConnectionFactory.getConf(config.connection);
        credentials = TwitterCredentials.credentialsFromFile(new File(config.credentials));
        scaler = gson.fromJson(new FileReader(new File(config.scaler)), Scaler.class);
        modelEndpoint = new ModelEndpoint(config.modelEndpoint, 5000);
        lsaFilename = config.lsaFilename;
        sparqlLocation = config.sparqlEndpoint;

        final ResourceConfig rc = new ResourceConfig().packages(Application.class.getPackage().getName());
        rc.register(new Binder());
        rc.register(new CORSResponseFilter());
        rc.register(new EncodingResponseFilter());
        rc.register(new GenericExceptionMapper());
        URI uri = new URI(null, null, "0.0.0.0", config.port, null, null, null);
        final HttpServer httpServer =  GrizzlyHttpServerFactory.createHttpServer(uri, rc);

        try {
            httpServer.start();
            Thread.currentThread().join();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class Binder extends AbstractBinder {

        @Override
        protected void configure() {
            bind(credentials).to(TwitterCredentials[].class);
            bind(alignments).to(DataSource.class);
            bind(sparqlLocation).named("SPARQLEndpoint").to(String.class);
            bind(ngramsEndpoint).named("NgramsEndpoint").to(String.class);
            bind(wikimachineEndpoint).named("WikimachineEndpoint").to(String.class);
            bind(lsaFilename).named("lsaFilename").to(String.class);
            bind(scaler).to(Scaler.class);
            bind(modelEndpoint).to(ModelEndpoint.class);
            bindFactory(TwitterFactory.class).to(Twitter[].class);
            bindFactory(ConnectionFactory.class).to(ConnectionProvider.class).in(Singleton.class);
            bind(AlignmentsService.class).to(AlignmentsService.class);
            bind(TwitterService.class).to(TwitterService.class);
            bind(KBAccessService.class).to(KBAccessService.class).in(Singleton.class);
            bind(WikimachineService.class).to(WikimachineService.class).in(Singleton.class);
            bind(AnnotationService.class).to(AnnotationService.class).in(Singleton.class);
            bind(MLService.class).to(MLService.class).in(Singleton.class);
            bind(ResourcesService.class).to(ResourcesService.class).in(Singleton.class);
            bind(NGramsService.class).to(NGramsService.class).in(Singleton.class);
            bind(OnlineAlignmentsService.class).to(OnlineAlignmentsService.class).in(Singleton.class);
        }
    }

    public static class GenericExceptionMapper implements ExceptionMapper<Throwable> {

        @Override
        public Response toResponse(Throwable ex) {

            ErrorMessage errorMessage = new ErrorMessage();
            setHttpStatus(ex, errorMessage);
            errorMessage.setMessage(ex.getMessage());
            StringWriter errorStackTrace = new StringWriter();
            ex.printStackTrace(new PrintWriter(errorStackTrace));
            errorMessage.setDeveloperMessage(errorStackTrace.toString());

            return Response.status(errorMessage.getStatus())
                    .entity(new Gson().toJson(errorMessage))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

        private void setHttpStatus(Throwable ex, ErrorMessage errorMessage) {
            if(ex instanceof WebApplicationException) {
                errorMessage.setStatus(((WebApplicationException)ex).getResponse().getStatus());
            } else {
                errorMessage.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()); //defaults to internal server error 500
            }
        }
    }

    public static class ErrorMessage {
        int status;
        int code;
        String message;
        String link;
        String developerMessage;

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getDeveloperMessage() {
            return developerMessage;
        }

        public void setDeveloperMessage(String developerMessage) {
            this.developerMessage = developerMessage;
        }

        public String getLink() {
            return link;
        }

        public void setLink(String link) {
            this.link = link;
        }

        public ErrorMessage() {}
    }

    public static class Configuration {
        public Integer port;
        public String connection;
        public String credentials;
        public String scaler;
        public String ngramsEndpoint = "redis://localhost:6379";
        public String wikimachineEndpoint = "http://ml.apnetwork.it/annotate";
        public String modelEndpoint = "localhost";
        public String sparqlEndpoint = "https://api.futuro.media/dbpedia/sparql";
        public String lsaFilename;
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
        options.addOption(
                Option.builder().desc("NGrams endpoint")
                        .hasArg().argName("uri").longOpt("ngrams").build()
        );
        options.addOption(
                Option.builder().desc("Model endpoint")
                        .hasArg().argName("uri").longOpt("model").build()
        );
        options.addOption(
                Option.builder().desc("Scaler serialisation")
                        .required().hasArg().argName("file").longOpt("scaler").build()
        );
        options.addOption(
                Option.builder().desc("LSA root filename")
                        .required().hasArg().argName("file").longOpt("lsa").build()
        );
        options.addOption(
                Option.builder().desc("SPARQL endpoint")
                        .hasArg().argName("uri").longOpt("sparql").build()
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
            config.scaler = line.getOptionValue("scaler");
            config.lsaFilename = line.getOptionValue("lsa");
            if (line.hasOption("model")) {
                config.modelEndpoint = line.getOptionValue("model");
            }
            if (line.hasOption("ngrams")) {
                config.ngramsEndpoint = line.getOptionValue("ngrams");
            }
            if (line.hasOption("sparql")) {
                config.sparqlEndpoint = line.getOptionValue("sparql");
            }
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
                "java -Dfile.encoding=UTF-8 "+Application.class.getName(),
                "\n",
                options,
                "\n",
                true
        );
    }
}
