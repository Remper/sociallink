package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.evaluation.DatasetEntry;
import eu.fbk.fm.alignments.kb.KBResource;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Data object containing everything needed to work with the entry
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FullyResolvedEntry implements Serializable {
    public DatasetEntry entry;
    public KBResource resource = null;
    public List<UserData> candidates = null;
    public List<Map<String, double[]>> features = new LinkedList<>();

    public FullyResolvedEntry(DatasetEntry entry) {
        this.entry = entry;
    }

    public FullyResolvedEntry(KBResource resource) {
        this.entry = new DatasetEntry(resource);
        this.resource = resource;
    }
}