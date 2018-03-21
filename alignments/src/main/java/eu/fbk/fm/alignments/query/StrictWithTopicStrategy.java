package eu.fbk.fm.alignments.query;


import eu.fbk.fm.alignments.DBpediaResource;

/**
 * Strict description adding the topic when exists
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class StrictWithTopicStrategy extends StrictStrategy {
    @Override
    public String getQuery(DBpediaResource resource) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getQuery(resource));
        String topic = resource.getTopicFromResourceId();
        if (topic.length() > 0) {
            sb.append(' ');
            sb.append(resource.getTopicFromResourceId());
        }
        return sb.toString();
    }
}
