package eu.fbk.fm.alignments.persistence.sparql;

import eu.fbk.fm.alignments.DBpediaResource;

import java.util.HashMap;

/**
 * Provides preconstructed DBpedia resource from the pool of preregistered ones
 */
public class FakeEndpoint implements ResourceEndpoint {
    HashMap<String, DBpediaResource> pool = new HashMap<>();

    @Override
    public DBpediaResource getResourceById(String resourceId) {
        return pool.get(resourceId);
    }

    public void register(DBpediaResource resource) {
        pool.put(resource.getIdentifier(), resource);
    }

    public void release(String resourceId) {
        pool.remove(resourceId);
    }
}
