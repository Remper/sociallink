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
import org.apache.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import static eu.fbk.fm.alignments.index.db.Tables.USER_SG;
import static eu.fbk.fm.alignments.index.db.Tables.KB_INDEX;

/**
 * Provides different types of embeddings from the embeddings API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class EmbeddingsProvider implements FeatureVectorProvider, JsonObjectProcessor {

    private static final Logger logger = Logger.getLogger(EmbeddingsProvider.class.getName());

    private final String embName;
    private final DataSource source;
    private final DSLContext context;
    private final String host = "localhost";
    private final int port = 5241;
    private final CloseableHttpClient client = HttpClients.createDefault();
    private URI url;

    private boolean enableWeights = true;

    public EmbeddingsProvider(DataSource source, String embName) throws URISyntaxException {
        this.embName = embName;
        this.source = source;
        this.context = DSL.using(source, SQLDialect.POSTGRES);
        init();
    }

    public EmbeddingsProvider turnOffWeights() {
        enableWeights = false;
        return this;
    }

    private void init() throws URISyntaxException {
        url = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath("/transform/"+embName).build();
    }

    public double[] predict(Long[] features) {
        return predict(features, null);
    }

    public double[] predict(Long[] features, Float[] weights) {
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
                response.close();
            }
            JsonObject object = gson.fromJson(new InputStreamReader(response.getEntity().getContent()), JsonObject.class);
            JsonArray results = get(object, JsonArray.class, "data", "embedding");
            result = new double[results.size()];
            int pointer = 0;
            for (JsonElement ele : results) {
                result[pointer] = ele.getAsDouble();
                pointer++;
            }
        } catch (URISyntaxException | IOException | JsonSyntaxException e) {
            e.printStackTrace();
            logger.error(e);
        }
        if (response != null) {
            try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (result == null) {
            return new double[0];
        }
        return result;
    }

    @Override
    public double[] getFeatures(User user, DBpediaResource resource) {
        if (this.embName.startsWith("sg")) {
            Record2<Long[], Float[]> userVectorRaw = context
                    .select(USER_SG.FOLLOWEES, USER_SG.WEIGHTS)
                    .from(USER_SG)
                    .where(USER_SG.UID.eq(user.getId()))
                    .fetchOne();

            if (userVectorRaw == null) {
                return predict(new Long[0]);
            }

            return predict(userVectorRaw.value1(), userVectorRaw.value2());
        }

        Long userVectorRaw = context
                .select(KB_INDEX.KBID)
                .from(KB_INDEX)
                .where(KB_INDEX.URI.eq(resource.getIdentifier()))
                .fetchOne(KB_INDEX.KBID, Long.class);

        if (userVectorRaw == null) {
            return predict(new Long[0]);
        }

        Long[] result = new Long[1];
        result[0] = userVectorRaw;

        return predict(result);
    }
}
