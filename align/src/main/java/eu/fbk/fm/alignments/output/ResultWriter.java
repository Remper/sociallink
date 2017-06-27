package eu.fbk.fm.alignments.output;

import eu.fbk.fm.alignments.index.db.tables.records.AlignmentsRecord;

import java.io.Closeable;
import java.util.List;

/**
 * A common interface for result writers
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface ResultWriter extends Closeable {
    void write(String resourceId, List<DumpResource.Candidate> candidates, Long trueUid);
    void flush();
}