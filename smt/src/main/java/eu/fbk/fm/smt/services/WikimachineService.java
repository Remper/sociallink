package eu.fbk.fm.smt.services;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.core.util.IOUtils;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * A service that fetches from thewikimachine API
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class WikimachineService {
    private String endpoint;
    private CloseableHttpClient client = null;

    @Inject
    public WikimachineService(@Named("WikimachineEndpoint") String url) throws
            KeyManagementException, NoSuchAlgorithmException, KeyStoreException, URISyntaxException {
        this.endpoint = url;
        init();
    }

    private void init() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        if (client == null) {
            client = HttpClients.custom()
                    .setSSLSocketFactory(new SSLConnectionSocketFactory(SSLContexts.custom()
                                    .loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> true)
                                    .build()
                            )
                    ).build();
        }
    }

    public String annotate(String text) throws URISyntaxException, IOException {
        URI requestURI = new URIBuilder(endpoint).build();
        HttpPost request = new HttpPost(requestURI);

        List<NameValuePair> urlParameters = new ArrayList<>();
        urlParameters.add(new BasicNameValuePair("text", text));
        urlParameters.add(new BasicNameValuePair("disambiguation", "1"));
        urlParameters.add(new BasicNameValuePair("topic", "0.25"));
        urlParameters.add(new BasicNameValuePair("include_text", "1"));
        urlParameters.add(new BasicNameValuePair("min_weight", "0.25"));
        urlParameters.add(new BasicNameValuePair("image", "1"));
        urlParameters.add(new BasicNameValuePair("class", "1"));
        urlParameters.add(new BasicNameValuePair("app_id", "0"));
        urlParameters.add(new BasicNameValuePair("app_key", "0"));

        request.setEntity(new UrlEncodedFormEntity(urlParameters));
        CloseableHttpResponse response = client.execute(request);
        if (response.getStatusLine().getStatusCode() >= 400) {
            response.close();
            throw new IOException("Wikimachine endpoint didn't understand the request");
        }
        String responseText = IOUtils.toString(new InputStreamReader(response.getEntity().getContent()));
        response.close();

        return responseText;
    }
}
