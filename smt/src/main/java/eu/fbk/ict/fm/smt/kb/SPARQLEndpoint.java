package eu.fbk.ict.fm.smt.kb;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Query the knowledge base for a data for the particular entity
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class SPARQLEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLEndpoint.class);
    private static final String PROPERTIES_FOR_ENTITY = "" +
            "select " +
            "  ?relation ?property " +
            "where { " +
            "  <:resourceId> ?relation ?property " +
            "} " +
            "group by ?property ?relation";

    private URI url;
    private CloseableHttpClient client = null;

    public SPARQLEndpoint(String endpointURL) throws URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        url = new URIBuilder(endpointURL).build();
        init();
    }

    private void init() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        if (client == null) {
            //TODO: properly add a correct certification authority
            client = HttpClients.custom()
                    .setSSLSocketFactory(new SSLConnectionSocketFactory(SSLContexts.custom()
                                    .loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> true)
                                    .build()
                            )
                    ).build();
        }
    }

    public CSVParser execute(String query) throws URISyntaxException, IOException {
        URI requestURI = getBuilder().setParameter("query", query)
                .setParameter("accept", "text/csv")
                .build();
        CloseableHttpResponse response = client.execute(new HttpGet(requestURI));
        if (response.getStatusLine().getStatusCode() >= 400) {
            response.close();
            throw new IOException("SPARQL endpoint didn't understand the request");
        }
        return new CSVParser(
                new BufferedReader(new InputStreamReader(response.getEntity().getContent())),
                CSVFormat.DEFAULT.withDelimiter(',').withHeader()
        );
    }

    public KBResource getResourceById(String resourceId) {
        Map<String, List<String>> properties = new HashMap<>();
        try {
            CSVParser parser = execute(PROPERTIES_FOR_ENTITY.replace(":resourceId", resourceId));
            for (CSVRecord record : parser) {
                List<String> propertyContainer = properties.getOrDefault(record.get("relation"), new LinkedList<>());
                propertyContainer.add(record.get("property"));
                properties.put(record.get("relation"), propertyContainer);
            }
            parser.close();
        } catch (URISyntaxException | IOException e) {
            logger.error("Can't request this entity info", e);
        }
        return new KBResource(resourceId, properties);
    }

    public URIBuilder getBuilder() {
        return new URIBuilder(url);
    }
}
