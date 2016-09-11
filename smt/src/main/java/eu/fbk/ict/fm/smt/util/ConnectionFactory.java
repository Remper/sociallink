package eu.fbk.ict.fm.smt.util;

import com.google.gson.Gson;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Provides SQL connection to services
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ConnectionFactory implements Factory<Connection> {
    private Credentials credentials;

    @Inject
    public ConnectionFactory(Credentials credentials) {
        this.credentials = credentials;
    }

    @Override
    public Connection provide() {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
            return DriverManager.getConnection(credentials.url, credentials.user, credentials.pass);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void dispose(Connection instance) {

    }

    public static class Credentials {
        public String user, pass, url;
    }

    public static ConnectionFactory.Credentials getConf(String file) throws FileNotFoundException {
        Gson gson = new Gson();
        return gson.fromJson(new FileReader(file), ConnectionFactory.Credentials.class);
    }
}
