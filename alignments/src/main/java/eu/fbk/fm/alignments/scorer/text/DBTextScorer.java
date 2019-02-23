package eu.fbk.fm.alignments.scorer.text;

import com.google.common.util.concurrent.AtomicDouble;
import eu.fbk.fm.alignments.kb.KBResource;
import eu.fbk.fm.alignments.scorer.FeatureProvider;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.User;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static eu.fbk.fm.alignments.index.db.tables.UserText.USER_TEXT;

/**
 * Compares entity's text to the text contained in the DB
 */
public class DBTextScorer implements FeatureProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBTextScorer.class);

    protected DataSource source;
    protected SimilarityScorer scorer;

    protected boolean verbose = false;

    protected AtomicInteger requests = new AtomicInteger();
    protected AtomicLong avgDescLength = new AtomicLong();
    protected AtomicDouble avgScore = new AtomicDouble();

    public DBTextScorer(DataSource source, SimilarityScorer scorer) {
        this.source = source;
        this.scorer = scorer;
    }

    @Override
    public double getFeature(User user, KBResource resource) {
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

        int curRequests = requests.getAndIncrement()+1;
        if (curRequests % 50000 == 0 && curRequests > 0) {
            LOGGER.info(String.format("[Subspace: %s] Processed %5d requests (avg. desc. length: %.2f, avg. sim. score: %.2f)", getSubspaceId(), curRequests, ((double)avgDescLength.get())/curRequests, avgScore.get()/curRequests));
            LOGGER.info(String.format("  [%s] [%s] [%s] [%s] [%s]", resource.getClass().getSimpleName(), resource.getIdentifier(), join(resource.getNames()), join(resource.getLabels()), join(resource.getDescriptions())));
            LOGGER.info(String.format(
                "  [@%s] [%d symbols: %s]",
                user.getScreenName(),
                userTextRaw.length(),
                userTextRaw.length() > 100 ? userTextRaw.substring(0, 100) : userTextRaw.length()
            ));
        }

        return process(userTextRaw, resource);
    }

    protected double process(String userText, KBResource resource) {
        String resourceText = resource.getDescriptions().stream().reduce((s1, s2) -> s1+" "+s2).orElse("");

        double curScore = scorer.score(userText, resourceText);
        avgDescLength.getAndAdd(resourceText.length());
        avgScore.getAndAdd(curScore);
        return curScore;
    }

    protected String join(List<String> list) {
        return list.stream().reduce((v1, v2) -> v1+", "+v2).orElse("<empty>");
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
}
