package eu.fbk.fm.alignments.index.sink;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.IOUtils;

import java.io.*;

/**
 * Outputs a postgres-compatible file
 *
 * @see <a href="https://www.postgresql.org/docs/current/static/sql-copy.html">COPY SQL command</a>}
 */
public class PostgresFileSink<T extends Tuple> implements OutputFormat<T> {

    private static final long serialVersionUID = 1L;

    private static final int[] toEscape = new int[]{'\r', '\n', '\\', '\t'};

    private transient String filename;
    private transient Writer writer;

    private final String appendix;

    public PostgresFileSink() {
        this("output");
    }

    public PostgresFileSink(String appendix) {
        this.appendix = appendix;
    }

    public PostgresFileSink<T> testFile(Configuration parameters) throws IOException {
        configure(parameters);
        final File testFile = prepareFile();
        if (!testFile.createNewFile() || !testFile.delete()) {
            throw new IOException("Can't make sure that the destination is writable");
        }

        return this;
    }

    @Override
    public void configure(Configuration parameters) {
        filename = parameters.getString("db.file", null);
    }

    private File prepareFile() {
        return prepareFile(0, 1);
    }

    private File prepareFile(int taskNumber, int numTasks) {
        if (numTasks == 1) {
            return new File(filename, appendix);
        } else {
            return new File(filename, appendix + "-" + taskNumber + "-" + numTasks);
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        final File file = prepareFile(taskNumber, numTasks);
        writer = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(file), 1024 * 1024));
    }

    private void write(int value) {
        try {
            if (ArrayUtils.contains(toEscape, value)) {
                writer.write('\\');
            }
            writer.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(T record) throws IOException {
        int size = record.getArity();

        for (int i = 0; i < size; i++) {
            String toWrite = record.getField(i).toString();
            toWrite.chars().forEach(this::write);
            if (i < size-1) {
                writer.write('\t');
            }
        }
        writer.write('\n');
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        IOUtils.closeQuietly(writer);
    }
}
