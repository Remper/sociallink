package eu.fbk.fm.alignments.index;

import eu.fbk.fm.alignments.query.QueryAssemblyStrategy;
import eu.fbk.fm.alignments.scorer.FullyResolvedEntry;
import org.apache.flink.util.IOUtils;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;
import java.util.function.Consumer;

import static eu.fbk.fm.alignments.index.db.tables.UserIndex.USER_INDEX;
import static eu.fbk.fm.alignments.index.db.tables.UserObjects.USER_OBJECTS;

/**
 * Use database to fill the list of candidates for FullyResolvedEntry
 */
public class FillFromIndex implements AutoCloseable {

    Connection connection;
    DSLContext context;
    QueryAssemblyStrategy qaStrategy;

    public FillFromIndex(QueryAssemblyStrategy qaStrategy, String connString, String connUser, String connPassword) throws IOException {
        this.qaStrategy = qaStrategy;

        try  {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(connString, connUser, connPassword);
            context = DSL.using(connection, SQLDialect.POSTGRES);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * SELECT
     ui.*,
     to_tsvector('english', ui.fullname) AS vector,
     uo.object->'screen_name'
     FROM
     user_index AS ui
     LEFT JOIN
     user_objects AS uo ON ui.uid = uo.uid
     WHERE
     plainto_tsquery('donald trump') @@ to_tsvector('english', fullname)
     ORDER BY freq DESC
     LIMIT 100;

     * @param entry
     */
    public void fill(FullyResolvedEntry entry) {
        entry.candidates = new LinkedList<>();

        context
            .select(USER_OBJECTS.fields())
            .from(USER_INDEX)
            .join(USER_OBJECTS)
            .on(USER_INDEX.UID.eq(USER_OBJECTS.UID))
            .where(
                    "plainto_tsquery({0}) @@ to_tsvector('english', USER_INDEX.FULLNAME)",
                    qaStrategy.getQuery(entry.resource)
            )
            .limit(10)
            .fetchStream()
            .forEach(new Consumer<Record>() {
                @Override
                public void accept(Record record) {
                    Object test = record.get(USER_OBJECTS.OBJECT);
                    test.toString();
                }
            });

        //TwitterObjectFactory.createStatus()
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(context);
        IOUtils.closeQuietly(connection);
    }
}
