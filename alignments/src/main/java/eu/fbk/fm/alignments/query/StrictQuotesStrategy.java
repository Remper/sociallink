package eu.fbk.fm.alignments.query;

import eu.fbk.fm.alignments.kb.KBResource;

/**
 * Same as StrictStrategy, but enforcing an exact match
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class StrictQuotesStrategy extends StrictStrategy {
    @Override
    public String getQuery(KBResource resource) {
        return '"' + super.getQuery(resource) + '"';
    }
}
