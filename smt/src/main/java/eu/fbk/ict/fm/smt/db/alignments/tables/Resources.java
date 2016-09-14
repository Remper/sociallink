/**
 * This class is generated by jOOQ
 */
package eu.fbk.ict.fm.smt.db.alignments.tables;


import eu.fbk.ict.fm.smt.db.alignments.Alignments;
import eu.fbk.ict.fm.smt.db.alignments.Keys;
import eu.fbk.ict.fm.smt.db.alignments.tables.records.ResourcesRecord;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.8.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Resources extends TableImpl<ResourcesRecord> {

    private static final long serialVersionUID = -677752463;

    /**
     * The reference instance of <code>alignments.resources</code>
     */
    public static final Resources RESOURCES = new Resources();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<ResourcesRecord> getRecordType() {
        return ResourcesRecord.class;
    }

    /**
     * The column <code>alignments.resources.resource_id</code>.
     */
    public final TableField<ResourcesRecord, String> RESOURCE_ID = createField("resource_id", org.jooq.impl.SQLDataType.VARCHAR.length(255).nullable(false), this, "");

    /**
     * The column <code>alignments.resources.is_dead</code>.
     */
    public final TableField<ResourcesRecord, Byte> IS_DEAD = createField("is_dead", org.jooq.impl.SQLDataType.TINYINT.nullable(false), this, "");

    /**
     * The column <code>alignments.resources.dataset</code>.
     */
    public final TableField<ResourcesRecord, String> DATASET = createField("dataset", org.jooq.impl.SQLDataType.VARCHAR.length(255).nullable(false).defaultValue(org.jooq.impl.DSL.inline("generic", org.jooq.impl.SQLDataType.VARCHAR)), this, "");

    /**
     * Create a <code>alignments.resources</code> table reference
     */
    public Resources() {
        this("resources", null);
    }

    /**
     * Create an aliased <code>alignments.resources</code> table reference
     */
    public Resources(String alias) {
        this(alias, RESOURCES);
    }

    private Resources(String alias, Table<ResourcesRecord> aliased) {
        this(alias, aliased, null);
    }

    private Resources(String alias, Table<ResourcesRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema getSchema() {
        return Alignments.ALIGNMENTS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<ResourcesRecord> getPrimaryKey() {
        return Keys.KEY_RESOURCES_PRIMARY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<ResourcesRecord>> getKeys() {
        return Arrays.<UniqueKey<ResourcesRecord>>asList(Keys.KEY_RESOURCES_PRIMARY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resources as(String alias) {
        return new Resources(alias, this);
    }

    /**
     * Rename this table
     */
    public Resources rename(String name) {
        return new Resources(name, null);
    }
}
