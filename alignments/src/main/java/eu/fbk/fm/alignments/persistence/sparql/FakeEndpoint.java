package eu.fbk.fm.alignments.persistence.sparql;

import eu.fbk.fm.alignments.kb.KBResource;

import java.util.HashMap;

/**
 * Provides preconstructed DBpedia resource from the pool of preregistered ones
 */
public class FakeEndpoint implements ResourceEndpoint {
    HashMap<String, KBResource> pool = new HashMap<>();

    @Override
    public KBResource getResourceById(String resourceId) {
        if (!pool.containsKey(resourceId)) {
            return getDefault(resourceId);
        }
        return pool.get(resourceId);
    }

    protected KBResource getDefault(String resourceId) {
        throw new RuntimeException("Resource should be registered before being requested");
    }

    public void register(KBResource resource) {
        pool.put(resource.getIdentifier(), resource);
    }

    public void release(String resourceId) {
        pool.remove(resourceId);
    }
}
