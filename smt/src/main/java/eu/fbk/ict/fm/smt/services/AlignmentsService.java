package eu.fbk.ict.fm.smt.services;

import eu.fbk.ict.fm.smt.db.alignments.tables.Alignments;
import eu.fbk.ict.fm.smt.db.alignments.tables.Profiles;
import eu.fbk.ict.fm.smt.db.alignments.tables.Resources;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.AlignmentsRecord;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ProfilesRecord;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ResourcesRecord;
import org.glassfish.grizzly.utils.Pair;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jvnet.hk2.annotations.Service;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides information about alignments from the DB
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service
public class AlignmentsService {
    private static final Alignments ALIGNMENTS = Alignments.ALIGNMENTS_;
    private static final Resources RESOURCES = Resources.RESOURCES;

    @Inject
    private ConnectionProvider connection;


    public List<Record2<String, Integer>> getAvailableDatasets() {
        return context()
            .select(RESOURCES.DATASET, RESOURCES.DATASET.count())
            .from(RESOURCES)
            .groupBy(RESOURCES.DATASET)
            .fetch();
    }

    public List<AlignmentsRecord> getRecordsByTwitterId(long twitterId) {
        return getRecordsByTwitterId(Arrays.asList(
            ALIGNMENTS.TWITTER_ID.eq(twitterId),
            RESOURCES.IS_DEAD.notEqual((byte) 1)
        ));
    }

    public List<AlignmentsRecord> getRecordsByTwitterId(long twitterId, Collection<String> whitelist) {
        if (whitelist == null || whitelist.size() == 0) {
            return getRecordsByTwitterId(twitterId);
        }

        return getRecordsByTwitterId(Arrays.asList(
            ALIGNMENTS.TWITTER_ID.eq(twitterId),
            RESOURCES.IS_DEAD.notEqual((byte) 1),
            RESOURCES.DATASET.in(whitelist)
        ));
    }

    private List<AlignmentsRecord> getRecordsByTwitterId(Collection<Condition> conditions) {
        Result<Record> records = context()
            .select()
            .from(ALIGNMENTS)
            .join(RESOURCES)
            .on(RESOURCES.RESOURCE_ID.eq(ALIGNMENTS.RESOURCE_ID))
            .where(conditions)
            .fetch();

        return records.stream().map(record -> record.into(ALIGNMENTS)).collect(Collectors.toList());
    }

    public List<AlignmentsRecord> getRecordsByResourceId(String resourceId) {
        return context()
            .selectFrom(ALIGNMENTS)
            .where(ALIGNMENTS.RESOURCE_ID.eq(resourceId))
            .fetch();
    }

    public ResourcesRecord getResourceById(String resourceId) {
        return context()
            .selectFrom(RESOURCES)
            .where(RESOURCES.RESOURCE_ID.eq(resourceId))
            .fetchOne();
    }

    public Long getIdByUsername(String username) {
        List<ProfilesRecord> profiles = context()
            .selectFrom(Profiles.PROFILES)
            .where(Profiles.PROFILES.USERNAME.eq(username.toLowerCase()))
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
