package eu.fbk.ict.fm.smt.services;

import eu.fbk.ict.fm.smt.db.alignments.tables.Alignments;
import eu.fbk.ict.fm.smt.db.alignments.tables.Profiles;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.AlignmentsRecord;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ProfilesRecord;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.inject.Inject;
import java.sql.Connection;
import java.util.List;

/**
 * Provides information about alignments from the DB
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class AlignmentsService {
    private Connection connection;

    @Inject
    public AlignmentsService(Connection connection) {
        this.connection = connection;
    }

    public List<AlignmentsRecord> getRecordsByTwitterId(long twitterId) {
        return context()
                .selectFrom(Alignments.ALIGNMENTS_)
                .where(Alignments.ALIGNMENTS_.TWITTER_ID.eq(twitterId))
                .fetch();
    }

    public List<AlignmentsRecord> getRecordsByResourceId(String resourceId) {
        return context()
                .selectFrom(Alignments.ALIGNMENTS_)
                .where(Alignments.ALIGNMENTS_.RESOURCE_ID.eq(resourceId))
                .fetch();
    }

    public Long getIdByUsername(String username) {
        List<ProfilesRecord> profiles = context()
                .selectFrom(Profiles.PROFILES)
                .where(Profiles.PROFILES.USERNAME.eq(username))
                .fetch();
        if (profiles.size() == 0) {
            return null;
        }
        return profiles.get(0).getTwitterId();
    }

    private DSLContext context() {
        return DSL.using(connection, SQLDialect.MYSQL);
    }
}
