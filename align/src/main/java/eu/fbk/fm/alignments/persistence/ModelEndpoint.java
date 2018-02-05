package eu.fbk.fm.alignments.persistence;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Temporary class for returning prediction results from the model endpoint
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class ModelEndpoint {
    private static final Logger logger = Logger.getLogger(ModelEndpoint.class.getName());

    private String host = "localhost";
    private int port = 5000;
    private URI url;
    private CloseableHttpClient client = HttpClients.createDefault();

    public ModelEndpoint(String host, int port) throws URISyntaxException {
        this.port = port;
        this.host = host;
        init();
    }

    public ModelEndpoint() throws URISyntaxException {
        init();
    }

    public void init() throws URISyntaxException {
        url = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath("/predict").build();
    }

    public double[] predict(Map<String, double[]> features) {
        return new double[0];
    }

    public double[] predict(double[] features) {
        Gson gson = new GsonBuilder().create();
        double[] result = null;
        CloseableHttpResponse response = null;
        try {
            URI requestURI = new URIBuilder(url).setParameter("features", gson.toJson(features))
                    .setParameter("accept", "text/csv")
                    .build();
            response = client.execute(new HttpGet(requestURI));
            if (response.getStatusLine().getStatusCode() >= 400) {
                response.close();
            }
            result = gson.fromJson(new InputStreamReader(response.getEntity().getContent()), double[].class);
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
}
