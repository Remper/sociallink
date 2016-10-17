package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.Evaluate;
import twitter4j.User;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Data object containing everything needed to work with the entry
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class FullyResolvedEntry implements Serializable {
    public Evaluate.DatasetEntry entry;
    public DBpediaResource resource = null;
    public List<User> candidates = null;
    public List<double[]> features = new LinkedList<>();

    public FullyResolvedEntry(Evaluate.DatasetEntry entry) {
        this.entry = entry;
    }

    public FullyResolvedEntry(DBpediaResource resource) {
        this.entry = new Evaluate.DatasetEntry(resource);
        this.resource = resource;
    }
}