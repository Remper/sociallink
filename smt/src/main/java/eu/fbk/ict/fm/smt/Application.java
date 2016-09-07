package eu.fbk.ict.fm.smt;

import eu.fbk.ict.fm.smt.api.ProfilesController;

import eu.fbk.ict.fm.smt.util.CORSResponseFilter;
import eu.fbk.ict.fm.smt.util.TwitterCredentials;
import eu.fbk.ict.fm.smt.util.TwitterFactory;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.jaxws.JaxwsHandler;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import twitter4j.Twitter;
import twitter4j.conf.ConfigurationBuilder;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Starting point for the web application
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Application {
    private static final int DEFAULT_PORT = 8080;
    private static TwitterCredentials credentials;

    public static void main(String[] args) throws URISyntaxException, FileNotFoundException {
        int port = Integer.parseInt(args[0]);
        credentials = TwitterCredentials.credentialsFromFile(new File(args[1]))[0];

        final ResourceConfig rc = new ResourceConfig().packages(Application.class.getPackage().getName());
        rc.register(new Binder());
        rc.register(new CORSResponseFilter());
        URI uri = new URI(null, null, "0.0.0.0", port, null, null, null);
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
            bindFactory(TwitterFactory.class).to(Twitter.class);
        }
    }
}
