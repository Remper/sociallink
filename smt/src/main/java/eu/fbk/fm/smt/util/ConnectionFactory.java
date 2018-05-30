package eu.fbk.fm.smt.util;

import com.google.gson.Gson;
import eu.fbk.fm.alignments.utils.DBUtils;
import org.jooq.ConnectionProvider;
import org.jooq.impl.DataSourceConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.function.Supplier;

/**
 * Provides SQL connection to services
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Singleton
public class ConnectionFactory implements Supplier<ConnectionProvider> {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactory.class);

    private DataSourceConnectionProvider provider;

    @Inject
    public ConnectionFactory(DataSource credentials) {
        provider = new DataSourceConnectionProvider(credentials);
    }

    @Override
    @Produces
    public ConnectionProvider get() {
        return provider;
    }

    public static class Credentials {
        public String user, pass, url;
    }

    public static DataSource getConf(String file) throws FileNotFoundException {
        Credentials credentials = new Gson().fromJson(new FileReader(file), Credentials.class);
        return DBUtils.createHikariDataSource(credentials.url, credentials.user, credentials.pass);
    }
}
