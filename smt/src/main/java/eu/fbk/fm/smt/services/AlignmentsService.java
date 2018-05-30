package eu.fbk.fm.smt.services;

import static eu.fbk.fm.alignments.index.db.tables.Alignments.ALIGNMENTS;
import static eu.fbk.fm.alignments.index.db.tables.UserObjects.USER_OBJECTS;

import eu.fbk.fm.alignments.index.db.tables.records.AlignmentsRecord;
import eu.fbk.fm.alignments.index.db.tables.records.UserObjectsRecord;
import org.jooq.*;
import org.jooq.impl.DSL;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides information about alignments from the DB
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@ApplicationScoped
public class AlignmentsService {

    @Inject
    private ConnectionProvider connection;

    public List<AlignmentsRecord> getRecordsByTwitterId(long twitterId) {
        return getRecordsByTwitterId(Collections.singletonList(
            ALIGNMENTS.UID.eq(twitterId)
        ));
    }

    private List<AlignmentsRecord> getRecordsByTwitterId(Collection<Condition> conditions) {
        Result<Record> records = context()
            .select()
            .from(ALIGNMENTS)
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

    public User getUserById(Long id) {
        UserObjectsRecord record = context()
                .selectFrom(USER_OBJECTS)
                .where(USER_OBJECTS.UID.eq(id))
                .fetchAny();

        if (record == null) {
            return null;
        }

        try {
            return TwitterObjectFactory.createUser(record.getObject().toString());
        } catch (TwitterException e) {
            return null;
        }
    }

    private DSLContext context() {
        return DSL.using(connection, SQLDialect.POSTGRES);
    }
}
