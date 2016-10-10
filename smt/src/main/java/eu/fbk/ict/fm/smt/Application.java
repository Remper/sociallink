package eu.fbk.ict.fm.smt;

import com.google.gson.Gson;
import eu.fbk.ict.fm.smt.services.*;
import eu.fbk.ict.fm.smt.util.CORSResponseFilter;
import eu.fbk.ict.fm.smt.util.ConnectionFactory;
import eu.fbk.ict.fm.smt.util.TwitterCredentials;
import eu.fbk.ict.fm.smt.util.TwitterFactory;
import org.apache.commons.cli.*;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jooq.ConnectionProvider;
import twitter4j.Twitter;

import javax.inject.Singleton;
import javax.sql.DataSource;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Starting point for the web application
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Application {
    private static TwitterCredentials credentials;
    private static DataSource alignments;
    private static String sparqlLocation = "https://api.futuro.media/dbpedia/sparql";

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
        rc.register(new GenericExceptionMapper());
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
            bind(alignments).to(DataSource.class);
            bind(sparqlLocation).named("SPARQLEndpoint").to(String.class);
            bindFactory(TwitterFactory.class).to(Twitter.class);
            bindFactory(ConnectionFactory.class).to(ConnectionProvider.class).in(Singleton.class);
            bind(AlignmentsService.class).to(AlignmentsService.class);
            bind(TwitterService.class).to(TwitterService.class);
            bind(KBAccessService.class).to(KBAccessService.class).in(Singleton.class);
            bind(MLService.class).to(MLService.class).in(Singleton.class);
            bind(ResourcesService.class).to(ResourcesService.class).in(Singleton.class);
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
