package eu.fbk.fm.smt;

import com.google.gson.Gson;
import eu.fbk.fm.alignments.persistence.ModelEndpoint;
import eu.fbk.fm.alignments.twitter.TwitterCredentials;
import eu.fbk.fm.smt.filters.CORSResponseFilter;
import eu.fbk.fm.smt.filters.EncodingResponseFilter;
import eu.fbk.fm.smt.util.ConnectionFactory;
import eu.fbk.utils.core.CommandLine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
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
    @Produces
    private static TwitterCredentials[] credentials;
    @Produces
    private static DataSource alignments;
    @Produces @Named("SPARQLEndpoint")
    private static String sparqlLocation;
    @Produces @Named("WikimachineEndpoint")
    private static String wikimachineEndpoint;
    @Produces @Named("NgramsEndpoint")
    private static String embeddingsEndpoint;
    @Produces @Named("lsaFilename")
    private static String lsaFilename;
    @Produces
    private static ModelEndpoint modelEndpoint;

    public static void main(String[] args) throws URISyntaxException, FileNotFoundException {
        Configuration config = loadConfiguration(args);
        if (config == null) {
            return;
        }
        Gson gson = new Gson();
        embeddingsEndpoint = config.embeddingsEndpoint;
        wikimachineEndpoint = config.wikimachineEndpoint;
        alignments = ConnectionFactory.getConf(config.connection);
        credentials = TwitterCredentials.credentialsFromFile(new File(config.credentials));
        modelEndpoint = new ModelEndpoint.ProductionModelEndpoint(config.modelEndpoint, 5000);
        lsaFilename = config.lsaFilename;
        sparqlLocation = config.sparqlEndpoint;

        final ResourceConfig rc = new ResourceConfig().packages(Application.class.getPackage().getName());
        //rc.register(new Binder());
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
        public Integer port = 5241;
        public String connection;
        public String credentials;
        public String embeddingsEndpoint = "redis://localhost:6379";
        public String wikimachineEndpoint = "http://ml.apnetwork.it/annotate";
        public String modelEndpoint = "localhost";
        public String sparqlEndpoint = "https://api.futuro.media/dbpedia/sparql";
        public String lsaFilename;
    }

    private static final String PORT_PARAM = "port";
    private static final String TWITTER_CREDENTIALS_PARAM = "credentials";
    private static final String DB_PARAM = "db";
    private static final String EMBEDDINGS_PARAM = "embeddings";
    private static final String MODEL_PARAM = "model";
    private static final String LSA_PARAM = "lsa";
    private static final String SPARQL_PARAM = "sparql";



    private static CommandLine.Parser provideParameterList() {
        return CommandLine.parser()
                .withOption("p", PORT_PARAM,
                        "Port on which to instantiate the application", "INT",
                        CommandLine.Type.STRING, true, false, false)
                .withOption("c", TWITTER_CREDENTIALS_PARAM,
                        "JSON file with Twitter credentials", "JSON",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, DB_PARAM,
                        "SocialLink Database credentials", "JSON",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, EMBEDDINGS_PARAM,
                        "URI of embeddings endpoint", "URI",
                        CommandLine.Type.STRING, true, false, false)
                .withOption(null, MODEL_PARAM,
                        "URI of SocialLink model endpoint", "URI",
                        CommandLine.Type.STRING, true, false, false)
                .withOption(null, LSA_PARAM,
                        "LSA root filename", "DIRECTORY+PREFIX",
                        CommandLine.Type.STRING, true, false, true)
                .withOption(null, SPARQL_PARAM,
                        "URI of the SPARQL endpoint", "URI",
                        CommandLine.Type.STRING, true, false, false);
    }

    private static Configuration loadConfiguration(String[] args) {
        try {
            // Parse command line
            final CommandLine cmd = provideParameterList().parse(args);

            Configuration config = new Configuration();
            config.port = cmd.getOptionValue(PORT_PARAM, Integer.class);
            config.connection = cmd.getOptionValue(DB_PARAM, String.class);
            config.credentials = cmd.getOptionValue(TWITTER_CREDENTIALS_PARAM, String.class);
            config.lsaFilename = cmd.getOptionValue(LSA_PARAM, String.class);
            if (cmd.hasOption(MODEL_PARAM)) {
                config.modelEndpoint = cmd.getOptionValue(MODEL_PARAM, String.class);
            }
            if (cmd.hasOption(EMBEDDINGS_PARAM)) {
                config.embeddingsEndpoint = cmd.getOptionValue(EMBEDDINGS_PARAM, String.class);
            }
            if (cmd.hasOption(SPARQL_PARAM)) {
                config.sparqlEndpoint = cmd.getOptionValue(SPARQL_PARAM, String.class);
            }
            return config;
        } catch (final Throwable ex) {
            // Handle exception
            CommandLine.fail(ex);
            System.exit(1);
        }
        return null;
    }
}
