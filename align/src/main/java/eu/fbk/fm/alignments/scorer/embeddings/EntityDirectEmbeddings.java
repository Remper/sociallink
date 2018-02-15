package eu.fbk.fm.alignments.scorer.embeddings;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.Serializable;
import java.net.URISyntaxException;

import static eu.fbk.fm.alignments.index.db.Tables.KB_INDEX;

/**
 * Queries embeddings endpoint based on Wikidata URI of an entity
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class EntityDirectEmbeddings extends EmbeddingsProvider {

    public EntityDirectEmbeddings(DataSource source, String embName) throws URISyntaxException {
        super(source, embName);
        if (!embName.startsWith("kb")) {
            logger.warn("KB-based embeddings should start with 'kb' but was '"+embName+"' found");
        }
    }

    @Override
    public double[] _getFeatures(User user, DBpediaResource resource) {
        return predict((Serializable[]) new String[]{resource.getIdentifier()});
    }
}
