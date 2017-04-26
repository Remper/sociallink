package eu.fbk.fm.alignments.index.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.TableRecord;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract class that implements commons methods for saving to the postgres DB
 */
public abstract class AbstractPostgresSink<T, R extends TableRecord<?>> implements OutputFormat<T> {
    protected transient String connString;
    protected transient String connUser;
    protected transient String connPassword;
    protected transient Connection connection;
    protected transient DSLContext context;

    protected List<R> buffer = new LinkedList<>();

    @Override
    public void configure(Configuration parameters) {
        connString = parameters.getString("db.connection", null);
        connUser = parameters.getString("db.user", null);
        connPassword = parameters.getString("db.password", null);

        if (connString == null || connUser == null || connPassword == null) {
            throw new InvalidParameterException("Required parameteres were not specified");
        }
    }

    protected void append(R element) {
        if (buffer.size() < 5000) {
            buffer.add(element);
            return;
        }

        flush();
    }

    private void flush() {
        context.batchInsert(buffer).execute();
        buffer.clear();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try  {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(connString, connUser, connPassword);
            context = DSL.using(connection, SQLDialect.POSTGRES);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
            context.close();
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
