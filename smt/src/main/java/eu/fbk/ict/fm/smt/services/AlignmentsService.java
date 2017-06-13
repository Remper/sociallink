package eu.fbk.ict.fm.smt.services;

import static eu.fbk.fm.alignments.index.db.tables.Alignments.ALIGNMENTS;

import eu.fbk.fm.alignments.index.db.tables.records.AlignmentsRecord;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Provides information about alignments from the DB
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
@Service
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

    private DSLContext context() {
        return DSL.using(connection, SQLDialect.POSTGRES);
    }
}
