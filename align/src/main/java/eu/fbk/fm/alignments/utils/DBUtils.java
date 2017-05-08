package eu.fbk.fm.alignments.utils;

import org.postgresql.ds.PGSimpleDataSource;

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
}
