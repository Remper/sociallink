package eu.fbk.fm.alignments.utils.flink;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class GzipTextOutputFormat<T> extends TextOutputFormat<T> {

    public GzipTextOutputFormat(Path outputPath) {
        super(outputPath);
    }

    public GzipTextOutputFormat(Path outputPath, String charset) {
        super(outputPath, charset);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);

        this.stream = new GZIPWrappedStream(this.stream, 4096, false);
    }

    @Override
    protected String getDirectoryFileName(int taskNumber) {
        return Integer.toString(taskNumber + 1)+".gz";
    }

    public static class GZIPWrappedStream extends FSDataOutputStream {

        private GZIPOutputStream stream;
        private FSDataOutputStream original;

        public GZIPWrappedStream(FSDataOutputStream original, int size, boolean syncFlush) throws IOException {
            this.stream = new GZIPOutputStream(original, size, syncFlush);
            this.original = original;
        }

        @Override
        public void write(final int b) throws IOException {
            this.stream.write(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            this.stream.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            this.stream.close();
        }


        @Override
        public void flush() throws IOException {
            this.stream.flush();
        }

        @Override
        public void sync() throws IOException {
            this.original.sync();
        }

        @Override
        public long getPos() throws IOException {
            return this.original.getPos();
        }
    }
}
