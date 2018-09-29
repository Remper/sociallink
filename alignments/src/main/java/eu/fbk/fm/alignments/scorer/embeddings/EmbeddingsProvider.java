package eu.fbk.fm.alignments.scorer.embeddings;

import com.google.gson.*;
import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.FeatureVectorProvider;
import eu.fbk.fm.alignments.utils.flink.JsonObjectProcessor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides different types of embeddings from the embeddings API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public abstract class EmbeddingsProvider implements FeatureVectorProvider, JsonObjectProcessor {

    protected static final Logger logger = LoggerFactory.getLogger(EmbeddingsProvider.class.getName());

    protected final String embName;
    private final DataSource source;
    private final String host = "localhost";
    private final int port = 5241;
    private final CloseableHttpClient client = HttpClients.createDefault();

    private final static int CACHE_LIMIT = 10000;
    private final LinkedList<String> cacheQueue = new LinkedList<>();
    private final HashMap<String, double[]> cache = new HashMap<>();

    private URI url;
    protected DSLContext context;

    private boolean enableWeights = true;

    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger resolved = new AtomicInteger();
    private final AtomicInteger zeroes = new AtomicInteger();

    public EmbeddingsProvider(DataSource source, String embName) throws URISyntaxException {
        if (embName.length() == 0) {
            throw new IllegalArgumentException("embName can't be empty");
        }

        this.embName = embName;
        this.source = source;
        init();
    }

    public String getSubspaceId() {
        return "emb_" + this.embName + (enableWeights ? "_w" : "");
    }

    public EmbeddingsProvider turnOffWeights() {
        enableWeights = false;
        return this;
    }

    private void init() throws URISyntaxException {
        context = DSL.using(source, SQLDialect.POSTGRES);
        url = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath("/transform/"+embName).build();
    }

    public double[] predict(Serializable[] features) {
        return predict(features, null);
    }

    public double[] predict(Serializable[] features, Float[] weights) {
        Gson gson = new GsonBuilder().create();
        double[] result = null;
        CloseableHttpResponse response = null;
        try {
            URIBuilder requestBuilder = new URIBuilder(url).setParameter("followees", gson.toJson(features));
            if (weights != null && enableWeights) {
                requestBuilder.setParameter("weights", gson.toJson(weights));
            }
            URI requestURI = requestBuilder.build();

            response = client.execute(new HttpGet(requestURI));
            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new IOException(String.format(
                        "Embeddings endpoint didn't understand the request. Code: %d",
                        response.getStatusLine().getStatusCode()
                ));
            }
            JsonObject object = gson.fromJson(new InputStreamReader(response.getEntity().getContent()), JsonObject.class);
            JsonArray results = get(object, JsonArray.class, "data", "embedding");
            if (results == null) {
                throw new IOException("Incorrect response: "+gson.toJson(object));
            }
            int curResolved = get(object, Integer.class, "data", "resolved");
            int totalResolved = resolved.getAndAdd(curResolved);
            int totalZeroes;
            if (curResolved == 0) {
                totalZeroes = zeroes.getAndIncrement();
            } else {
                totalZeroes = zeroes.get();
            }

            int curRequests = requests.getAndIncrement();
            if (curRequests % 50000 == 0 && curRequests > 0) {
                logger.info(String.format("[Subspace: %s] Processed %5d requests (%5d zeroes, %.2f avg. resolved)", getSubspaceId(), curRequests, totalZeroes, (float) totalResolved / curRequests));
            }
            result = new double[results.size()];
            int pointer = 0;
            for (JsonElement ele : results) {
                result[pointer] = ele.getAsDouble();
                pointer++;
            }
        } catch (URISyntaxException | IOException | JsonSyntaxException e) {
            e.printStackTrace();
            logger.error(String.format(
                    "Error in subspace %s while requesting for followees %s and weights %s",
                    getSubspaceId(),
                    gson.toJson(features), gson.toJson(weights)
            ), e);
        }

        //Checking if everything is properly closed
        if (response != null) {
            try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //Returning empty result in case of an error
        if (result == null) {
            return new double[0];
        }
        return result;
    }

    private String usrRsrcKey(User user, DBpediaResource resource) {
        return Long.toString(user.getId()) + resource.getIdentifier();
    }

    @Override
    public double[] getFeatures(User user, DBpediaResource resource) {
        String key = usrRsrcKey(user, resource);
        if (cache.containsKey(key)) {
            return cache.get(key);
        }

        double[] features = _getFeatures(user, resource);
        if (cache.size() >= CACHE_LIMIT) {
            cache.remove(cacheQueue.pollFirst());
        }
        cache.put(key, features);
        cacheQueue.add(key);

        return features;
    }

    public abstract double[] _getFeatures(User user, DBpediaResource resource);
}
