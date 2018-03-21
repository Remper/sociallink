package eu.fbk.fm.alignments.utils;

import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.Properties;

/**
 * Utility class for DB manipulation
 */
public class DBUtils {
    public static PGSimpleDataSource createPGDataSource(String connString, String connUser, String connPassword) {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setUrl(connString);
        source.setUser(connUser);
        source.setPassword(connPassword);
        return source;
    }

    public static HikariDataSource createHikariDataSource(String connString, String connUser, String connPassword){

        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(connString);
        ds.setUsername(connUser);
        ds.setPassword(connPassword);
        ds.setMaximumPoolSize(10);
        ds.setIdleTimeout(28740000);
        ds.setMaxLifetime(28740000);
        //ds.addDataSourceProperty("ssl.mode", "disable");

        return ds;
    }
}
