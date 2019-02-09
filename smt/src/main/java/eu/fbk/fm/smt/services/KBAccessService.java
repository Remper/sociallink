package eu.fbk.fm.smt.services;

import eu.fbk.fm.alignments.kb.DBpediaSpec;
import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.kb.ResourceSpec;
import eu.fbk.fm.alignments.persistence.sparql.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Service that requests additional data about entities from the knowledge base
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class KBAccessService {
    private static final Logger logger = LoggerFactory.getLogger(KBAccessService.class);
    private static final int CACHE_SIZE = 10000;

    private Endpoint endpoint;
    private LinkedList<String> cacheOrder = new LinkedList<>();
    private HashMap<String, KBResource> cache = new HashMap<>(CACHE_SIZE);


    @Inject
    public KBAccessService(@Named("SPARQLEndpoint") String url) throws
            KeyManagementException, NoSuchAlgorithmException, KeyStoreException, URISyntaxException {
        this.endpoint = new Endpoint(url);
    }

    public String getType(String resourceId) {
       KBResource resource = getResource(resourceId);
       return resource.getType().toString();
    }

    public synchronized KBResource getResource(String resourceId) {
        if (cache.containsKey(resourceId)) {
            logger.debug("KB cache("+cache.size()+"): hit");
            return cache.get(resourceId);
        }

        logger.debug("KB cache("+cache.size()+"): miss");
        KBResource resource = endpoint.getResourceById(resourceId);
        if (cache.size() == CACHE_SIZE) {
            String deleteId = cacheOrder.pollLast();
            cache.remove(deleteId);
        }
        cacheOrder.addFirst(resourceId);
        cache.put(resourceId, resource);
        return resource;
    }
}
