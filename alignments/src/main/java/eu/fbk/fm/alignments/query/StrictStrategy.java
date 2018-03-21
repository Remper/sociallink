package eu.fbk.fm.alignments.query;


import eu.fbk.fm.alignments.DBpediaResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Strict strategy that selects a single most popular name
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class StrictStrategy implements QueryAssemblyStrategy {
    @Override
    public String getQuery(DBpediaResource resource) {
        List<String> names = resource.getProperty(DBpediaResource.ATTRIBUTE_NAME);
        String cleanId = resource.getCleanResourceId();
        if (cleanId.length() > 0) {
            names.add(cleanId);
        }

        Map<String, Integer> counts = new HashMap<>();
        boolean isPerson = resource.isPerson();
        for (String name : names) {
            if (isPerson && name.contains(", ")) {
                continue;
            }
            counts.put(name, counts.getOrDefault(name, 0) + 1);
        }
        String maxKey = null;
        int maxCount = 0;
        for (String key : counts.keySet()) {
            int curCount = counts.get(key);
            if (maxCount < curCount) {
                maxCount = curCount;
                maxKey = key;
            }
        }
        if (maxKey == null) {
            return cleanId;
        }
        return maxKey;
    }
}
