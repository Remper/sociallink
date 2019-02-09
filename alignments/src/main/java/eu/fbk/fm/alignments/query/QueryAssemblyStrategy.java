package eu.fbk.fm.alignments.query;

import eu.fbk.fm.alignments.kb.KBResource;

/**
 * Defines a way to assemble a query
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface QueryAssemblyStrategy {
    String getQuery(KBResource resource);
    default String getQuery(KBResource resource, int option) {
        return getQuery(resource);
    }
}
