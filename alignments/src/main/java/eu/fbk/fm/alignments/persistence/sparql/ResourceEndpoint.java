package eu.fbk.fm.alignments.persistence.sparql;

import eu.fbk.fm.alignments.DBpediaResource;

/**
 * Provides DBpediaResource object by resource Id
 */
public interface ResourceEndpoint {
    DBpediaResource getResourceById(String resourceId);
}
