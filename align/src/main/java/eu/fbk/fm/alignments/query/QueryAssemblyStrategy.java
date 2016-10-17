package eu.fbk.fm.alignments.query;

import eu.fbk.fm.alignments.DBpediaResource;

/**
 * Defines a way to assemble a query
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public interface QueryAssemblyStrategy {
    String getQuery(DBpediaResource resource);
}
