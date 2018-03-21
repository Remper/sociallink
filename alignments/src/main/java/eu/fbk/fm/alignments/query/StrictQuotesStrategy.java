package eu.fbk.fm.alignments.query;

import eu.fbk.fm.alignments.DBpediaResource;

/**
 * Same as StrictStrategy, but enforcing an exact match
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class StrictQuotesStrategy extends StrictStrategy {
    @Override
    public String getQuery(DBpediaResource resource) {
        return '"' + super.getQuery(resource) + '"';
    }
}
