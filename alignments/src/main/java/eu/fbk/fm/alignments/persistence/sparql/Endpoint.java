package eu.fbk.fm.alignments.persistence.sparql;

import eu.fbk.fm.alignments.kb.DBpediaSpec;
import eu.fbk.fm.alignments.kb.KBResource;
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
 * Class that conveniently wraps HttpClient and request builder to work with a particular Virtuoso endpoint
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class Endpoint implements ResourceEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(Endpoint.class.getName());
    private static final String[] PROPERTIES_FOR_ENTITY = new String[]{"" +
        "select " +
        "  ?relation ?property " +
        "where { " +
        "  <:resourceId> ?relation ?property ",
        "} " +
        "group by ?property ?relation"};
    private static final String LANGUAGE_FILTER = "  FILTER (!ISLITERAL(?property) || LANG(?property) = '' || LANG(?property) = 'en') ";

    private URI url;
    private CloseableHttpClient client = null;
    private boolean englishOnly = true;

    public Endpoint(String endpointURL) throws URISyntaxException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        url = new URIBuilder(endpointURL).build();
        init();
    }

    public void setEnglishOnly(boolean englishOnly) {
        this.englishOnly = englishOnly;
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

    private CloseableHttpResponse executeQuery(String query) throws URISyntaxException, IOException {
        URI requestURI = getBuilder().setParameter("query", query)
                .setParameter("accept", "text/csv")
                .build();
        return client.execute(new HttpGet(requestURI));
    }

    public CSVParser process(CloseableHttpResponse response) throws URISyntaxException, IOException {
        if (response.getStatusLine().getStatusCode() >= 400) {
            IOException exception = new IOException(String.format("SPARQL endpoint didn't understand the request. Code: %d",
                                                response.getStatusLine().getStatusCode()));
            response.close();
            throw exception;
        }
        return new CSVParser(
                new BufferedReader(new InputStreamReader(response.getEntity().getContent())),
                CSVFormat.DEFAULT.withDelimiter(',').withHeader()
        );
    }

    public KBResource getResourceById(String resourceId) {
        Map<String, List<String>> properties = new HashMap<>();
        CloseableHttpResponse response = null;
        try {
            response = executeQuery(getQuery().replace(":resourceId", resourceId));

            for (CSVRecord record : process(response)) {
                List<String> propertyContainer = properties.getOrDefault(record.get("relation"), new LinkedList<>());
                propertyContainer.add(record.get("property"));
                properties.put(record.get("relation"), propertyContainer);
            }
        } catch (URISyntaxException | IOException e) {
            logger.error(String.format("Error while querying KB with resource ID %s", resourceId), e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return new KBResource(resourceId, new DBpediaSpec(), properties);
    }

    public URIBuilder getBuilder() {
        return new URIBuilder(url);
    }

    public String getQuery() {
        assert PROPERTIES_FOR_ENTITY.length == 2;

        StringBuilder sb = new StringBuilder();
        sb.append(PROPERTIES_FOR_ENTITY[0]);
        if (!englishOnly) {
            sb.append(LANGUAGE_FILTER);
        }
        sb.append(PROPERTIES_FOR_ENTITY[1]);
        return sb.toString();
    }
}
