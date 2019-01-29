package eu.fbk.fm.alignments.scorer.text;

import eu.fbk.fm.alignments.DBpediaResource;
import eu.fbk.fm.alignments.scorer.FeatureProvider;
import eu.fbk.fm.alignments.scorer.TextScorer;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.util.List;

import static eu.fbk.fm.alignments.index.db.tables.UserText.USER_TEXT;

/**
 * Compares entity's text to the text contained in the DB
 */
public class DBTextScorerv2 implements FeatureProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBTextScorerv2.class);

    protected DataSource source;
    protected SimilarityScorer scorer;

    protected boolean verbose = false;

    public DBTextScorerv2(DataSource source, SimilarityScorer scorer) {
        this.source = source;
        this.scorer = scorer;
    }

    @Override
    public double getFeature(User user, DBpediaResource resource) {
        String userTextRaw;
        try {
             userTextRaw = DSL.using(source, SQLDialect.POSTGRES)
                .select(USER_TEXT.TEXT)
                .from(USER_TEXT)
                .where(USER_TEXT.UID.eq(user.getId()))
                .fetchOne(USER_TEXT.TEXT);
        } catch (Exception e) {
            LOGGER.error("Something happened while querying user "+user.getScreenName(), e);
            throw e;
        }

        if (userTextRaw == null) {
            if (verbose) {
                LOGGER.debug("Can't find text for user: @"+user.getScreenName()+" ("+user.getId()+")");
            }
            return 0.0d;
        }
        if (verbose && userTextRaw.length() <= 5) {
            LOGGER.warn("Extremely short text for user: @"+user.getScreenName()+" ("+user.getId()+")");
        }

        return process(userTextRaw, resource);
    }

    protected double process(String userText, DBpediaResource resource) {
        List<String> resourceTexts = resource.getDescriptions();

        double topScore = 0.0d;
        for (String text : resourceTexts) {
            double curScore = scorer.score(userText, text);
            if (curScore > topScore) {
                topScore = curScore;
            }
        }
        return topScore;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
}
