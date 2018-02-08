package eu.fbk.fm.alignments.scorer.embeddings;

import eu.fbk.fm.alignments.DBpediaResource;
import org.jooq.Record2;
import twitter4j.User;

import javax.sql.DataSource;
import java.net.URISyntaxException;

import static eu.fbk.fm.alignments.index.db.Tables.USER_SG;

/**
 * Queries user_sg table in the database and then queries embeddings endpoint
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class SocialGraphEmbeddings extends EmbeddingsProvider {

    public SocialGraphEmbeddings(DataSource source, String embName) throws URISyntaxException {
        super(source, embName);
        if (!embName.startsWith("sg")) {
            logger.warn("Social graph-based embeddings should start with 'sg' but was '"+embName+"' found");
        }
    }

    @Override
    public double[] _getFeatures(User user, DBpediaResource resource) {
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
}
