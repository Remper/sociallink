package eu.fbk.fm.alignments.utils.flink;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

/**
 * This is an OutputFormat to serialize {@link Tuple}s to text. The output is
 * structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 */
@PublicEvolving
public class RobustTsvOutputFormat<T extends Tuple> extends FileOutputFormat<T> implements InputTypeConfigurable {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(RobustTsvOutputFormat.class);

    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.withQuote(null).withDelimiter('\t');

    private static Gson gson = new Gson();

    private transient CSVPrinter wrt;
    private boolean enableGzip;

    private final IntCounter nullCounter = new IntCounter();

    // --------------------------------------------------------------------------------------------
    // Constructors and getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    /**
     * Creates an instance of CsvOutputFormat.
     *
     * @param outputPath The path where the CSV file is written.
     */
    public RobustTsvOutputFormat(Path outputPath) {
        this(outputPath, false);
    }

    public RobustTsvOutputFormat(Path outputPath, boolean enableGzip) {
        super(outputPath);
        this.enableGzip = enableGzip;
    }

    // --------------------------------------------------------------------------------------------

    private String sanitizePathName(Path outputPath) {
        return outputPath.toString().replaceAll("[^a-zA-Z]+", "-");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        OutputStream outputStream;
        if (this.enableGzip) {
            outputStream = new GZIPOutputStream(this.stream, 4096, false);
        } else {
            outputStream = new BufferedOutputStream(this.stream, 4096);
        }
        this.wrt = new CSVPrinter(new OutputStreamWriter(outputStream), FORMAT);

        String nullCounterName = sanitizePathName(outputFilePath);
        if (nullCounterName.charAt(0) != '-') {
            nullCounterName = "-"+nullCounterName;
        }
        nullCounterName = "null-counter"+nullCounterName;

        getRuntimeContext().addAccumulator(nullCounterName, this.nullCounter);
    }

    @Override
    public void close() throws IOException {
        if (wrt != null) {
            this.wrt.flush();
            this.wrt.close();
        }
        super.close();
    }

    @Override
    public void writeRecord(T element) throws IOException {
        int numFields = element.getArity();

        boolean nullDetected = false;
        for (int i = 0; i < numFields; i++) {
            Object field = element.getField(i);

            String ele = "<unk>";
            if (field != null) {
                ele = field
                        .toString()
                        .replaceAll("\\s+", " ")
                        .trim();
            } else {
                nullDetected = true;
            }

            if (ele.length() == 0) {
                ele = " ";
            }

            this.wrt.print(ele);
        }
        this.wrt.println();

        if (nullDetected) {
            this.nullCounter.add(1);
        }
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "RobustTsvOutputFormat (path: " + this.getOutputFilePath() + ")";
    }

    /**
     *
     * The purpose of this method is solely to check whether the data type to be processed
     * is in fact a tuple type.
     */
    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (!type.isTupleType()) {
            throw new InvalidProgramException("The " + RobustTsvOutputFormat.class.getSimpleName() +
                    " can only be used to write tuple data sets.");
        }
    }
}
