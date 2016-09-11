/**
 * This class is generated by jOOQ
 */
package eu.fbk.ict.fm.smt.db.alignments;


import eu.fbk.ict.fm.smt.db.alignments.tables.Alignments;
import eu.fbk.ict.fm.smt.db.alignments.tables.Profiles;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.AlignmentsRecord;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ProfilesRecord;

import javax.annotation.Generated;

import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;


/**
 * A class modelling foreign key relationships between tables of the <code>alignments</code> 
 * schema
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.8.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // IDENTITY definitions
    // -------------------------------------------------------------------------


    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<AlignmentsRecord> KEY_ALIGNMENTS_PRIMARY = UniqueKeys0.KEY_ALIGNMENTS_PRIMARY;
    public static final UniqueKey<ProfilesRecord> KEY_PROFILES_PRIMARY = UniqueKeys0.KEY_PROFILES_PRIMARY;

    // -------------------------------------------------------------------------
    // FOREIGN KEY definitions
    // -------------------------------------------------------------------------


    // -------------------------------------------------------------------------
    // [#1459] distribute members to avoid static initialisers > 64kb
    // -------------------------------------------------------------------------

    private static class UniqueKeys0 extends AbstractKeys {
        public static final UniqueKey<AlignmentsRecord> KEY_ALIGNMENTS_PRIMARY = createUniqueKey(Alignments.ALIGNMENTS_, "KEY_alignments_PRIMARY", Alignments.ALIGNMENTS_.RESOURCE_ID, Alignments.ALIGNMENTS_.TWITTER_ID);
        public static final UniqueKey<ProfilesRecord> KEY_PROFILES_PRIMARY = createUniqueKey(Profiles.PROFILES, "KEY_profiles_PRIMARY", Profiles.PROFILES.TWITTER_ID);
    }
}
