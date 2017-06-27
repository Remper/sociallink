package eu.fbk.fm.alignments.evaluation;

import eu.fbk.fm.alignments.DBpediaResource;

/**
 * A single entity -> twitter_id alignment
 */
public class DatasetEntry {
    public String resourceId;
    public String twitterId;

    public DatasetEntry(String resourceId, String twitterId) {
        this.resourceId = resourceId;
        this.twitterId = twitterId;
    }

    public DatasetEntry(DBpediaResource resource) {
        this.resourceId = resource.getIdentifier();
        this.twitterId = null;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DatasetEntry)) {
            return super.equals(obj);
        }
        return resourceId.equals(((DatasetEntry) obj).resourceId);
    }

    @Override
    public int hashCode() {
        return resourceId.hashCode();
    }
}
