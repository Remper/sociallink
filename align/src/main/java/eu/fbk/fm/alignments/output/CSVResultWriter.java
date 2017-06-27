package eu.fbk.fm.alignments.output;

import com.google.common.base.Charsets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Writes CSV with the results to disk
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class CSVResultWriter implements ResultWriter {
    private final CSVPrinter csvWriter;

    public CSVResultWriter(File output) throws IOException {
        csvWriter = new CSVPrinter(
                new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(output)), Charsets.UTF_8),
                CSVFormat.DEFAULT);
    }

    @Override
    public void write(String resourceId, List<DumpResource.Candidate> candidates, Long trueUid) {
        try {
            for (DumpResource.Candidate candidate : candidates) {
                csvWriter.printRecord(resourceId, candidate.uid, candidate.score, candidate.is_alignment);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        try {
            csvWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        csvWriter.close();
    }
}
