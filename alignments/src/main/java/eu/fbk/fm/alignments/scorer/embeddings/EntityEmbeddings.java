package eu.fbk.fm.alignments.scorer.embeddings;

import eu.fbk.fm.alignments.DBpediaResource;
import twitter4j.User;

import javax.sql.DataSource;
import java.io.Serializable;
import java.net.URISyntaxException;

import static eu.fbk.fm.alignments.index.db.Tables.KB_INDEX;

/**
 * Queries kb_index table in the database and then queries embeddings endpoint
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class EntityEmbeddings extends EmbeddingsProvider {

    public EntityEmbeddings(DataSource source, String embName) throws URISyntaxException {
        super(source, embName);
        if (!embName.startsWith("kb")) {
            logger.warn("KB-based embeddings should start with 'kb' but was '"+embName+"' found");
        }
    }

    @Override
    public double[] _getFeatures(User user, DBpediaResource resource) {
        Long userVectorRaw = context
                .select(KB_INDEX.KBID)
                .from(KB_INDEX)
                .where(KB_INDEX.URI.eq(resource.getIdentifier()))
                .fetchOne(KB_INDEX.KBID, Long.class);

        if (userVectorRaw == null) {
            return predict((Serializable[]) new Long[0]);
        }

        Long[] result = new Long[1];
        result[0] = userVectorRaw;

        return predict((Serializable[]) result);
    }
}
