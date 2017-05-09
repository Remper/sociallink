package eu.fbk.fm.alignments.scorer;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.index.db.tables.UserText;
import eu.fbk.fm.alignments.scorer.text.LSAVectorProvider;
import eu.fbk.fm.alignments.scorer.text.VectorProvider;
import eu.fbk.utils.math.DenseVector;
import eu.fbk.utils.math.Vector;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;

import java.util.List;

import static eu.fbk.fm.alignments.index.db.tables.UserText.USER_TEXT;

/**
 * Compares entity's text to the precomputed LSA in the DB
 */
public class DBTextScorer implements FeatureProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBTextScorer.class);

    protected DataSource source;
    protected VectorProvider lsaVectorProvider;

    public DBTextScorer(DataSource source, LSAVectorProvider lsaVectorProvider) {
        this.source = source;
        this.lsaVectorProvider = lsaVectorProvider;
    }

    @Override
    public double getFeature(User user, DBpediaResource resource) {
        PGobject userVectorRaw = DSL.using(source, SQLDialect.POSTGRES)
                .select(USER_TEXT.LSA)
                .from(USER_TEXT)
                .where(USER_TEXT.UID.eq(user.getId()))
                .fetchOne(USER_TEXT.LSA, PGobject.class);


        if (userVectorRaw == null) {
            //LOGGER.debug("Can't find LSA for user: @"+user.getScreenName()+" ("+user.getId()+")");
            return 0.0d;
        }

        DenseVector userVector = new DenseVector(cubeToFloat(userVectorRaw));
        List<String> resourceTexts = TextScorer.getResourceTexts(resource);

        double topScore = 0.0d;
        for (String text : resourceTexts) {
            Vector textVector = lsaVectorProvider.toVector(text);
            double curScore = cosineSimilarity(userVector, (DenseVector) textVector);
            if (curScore > topScore) {
                topScore = curScore;
            }
        }
        return topScore;
    }

    private static float[] cubeToFloat(PGobject object) {
        String cubeString = object.getValue();
        String[] cubeArray = cubeString.substring(1, cubeString.length() - 1).split(", ");
        float[] target = new float[cubeArray.length];
        for (int i = 0; i < cubeArray.length; i++) {
            target[i] = Float.valueOf(cubeArray[i]);
        }

        return target;
    }

    private static float[] doubleToFloat(Double[] source) {
        float[] target = new float[source.length];
        for (int i = 0; i < target.length; i++) {
            target[i] = source[i].floatValue();
        }

        return target;
    }

    private static double cosineSimilarity(DenseVector v1, DenseVector v2) {
        double norm = Math.sqrt(v1.dotProduct(v1) * v2.dotProduct(v2));
        if (norm == 0.0d) {
            return 0.0d;
        }

        return v1.dotProduct(v2) / norm;
    }
}
