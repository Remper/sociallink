package eu.fbk.fm.alignments.persistence.sparql;

import eu.fbk.fm.alignments.kb.KBResource;

/**
 * Provides DBpediaResource object by resource Id
 */
public interface ResourceEndpoint {
    KBResource getResourceById(String resourceId);
}
