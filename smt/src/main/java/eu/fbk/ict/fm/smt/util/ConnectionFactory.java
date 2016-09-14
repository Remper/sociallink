package eu.fbk.ict.fm.smt.util;

import com.google.gson.Gson;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.glassfish.hk2.api.Factory;
import org.jooq.ConnectionProvider;
import org.jooq.impl.DataSourceConnectionProvider;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Provides SQL connection to services
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Singleton
public class ConnectionFactory implements Factory<ConnectionProvider> {
    private DataSourceConnectionProvider provider;

    @Inject
    public ConnectionFactory(DataSource credentials) {
        try {
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
            provider = new DataSourceConnectionProvider(credentials);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ConnectionProvider provide() {
        return provider;
    }

    @Override
    public void dispose(ConnectionProvider instance) {
    }

    public static class Credentials {
        public String user, pass, url;
    }

    public static DataSource getConf(String file) throws FileNotFoundException {
        Gson gson = new Gson();
        MysqlDataSource datasource = new MysqlDataSource();
        Credentials credentials = gson.fromJson(new FileReader(file), Credentials.class);
        datasource.setURL(credentials.url);
        datasource.setUser(credentials.user);
        datasource.setPassword(credentials.pass);
        return datasource;
    }
}
