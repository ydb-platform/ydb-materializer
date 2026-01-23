package tech.ydb.mv.model;

import tech.ydb.table.values.Type;

/**
 * Column of the materialized view table.
 *
 * @author zinal
 */
public class MvColumn implements MvSqlPosHolder {

    private final String name;
    private Type type;
    private String sourceAlias;
    private String sourceColumn;
    private MvJoinSource sourceRef;
    private MvComputation computation;
    private final MvSqlPos sqlPos;

    /**
     * Create a column with a known SQL position.
     *
     * @param name Column name.
     * @param sqlPos Position in the SQL text.
     */
    public MvColumn(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
    }

    /**
     * Create a column with an unknown/empty SQL position.
     *
     * @param name Column name.
     */
    public MvColumn(String name) {
        this(name, MvSqlPos.EMPTY);
    }

    /**
     * Check whether this column is a reference to a column in some input table.
     *
     * @return {@code true} if the column is a reference to a source table
     * column.
     */
    public boolean isReference() {
        return (sourceAlias != null) && (sourceColumn != null);
    }

    /**
     * Check whether this column is computed.
     *
     * @return {@code true} if the column value is computed (expression or
     * literal).
     */
    public boolean isComputation() {
        return (computation != null);
    }

    /**
     * Get column name.
     *
     * @return Column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Get column type.
     *
     * @return Column type (may be {@code null} until resolved).
     */
    public Type getType() {
        return type;
    }

    /**
     * Set column type.
     *
     * @param type Column type.
     */
    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Get source alias.
     *
     * @return Source table alias for reference columns.
     */
    public String getSourceAlias() {
        return sourceAlias;
    }

    /**
     * Set source alias.
     *
     * @param sourceAlias Source table alias for reference columns.
     */
    public void setSourceAlias(String sourceAlias) {
        this.sourceAlias = sourceAlias;
    }

    /**
     * Get source column name.
     *
     * @return Source column name for reference columns.
     */
    public String getSourceColumn() {
        return sourceColumn;
    }

    /**
     * Set source column name.
     *
     * @param sourceColumn Source column name for reference columns.
     */
    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    /**
     * Get resolved join source reference.
     *
     * @return Resolved join source reference (may be {@code null} until
     * resolved).
     */
    public MvJoinSource getSourceRef() {
        return sourceRef;
    }

    /**
     * Set resolved join source reference.
     *
     * @param sourceRef Resolved join source reference.
     */
    public void setSourceRef(MvJoinSource sourceRef) {
        this.sourceRef = sourceRef;
    }

    /**
     * Get computation definition.
     *
     * @return Computation definition (may be {@code null} if the column is not
     * computed).
     */
    public MvComputation getComputation() {
        return computation;
    }

    /**
     * Set computation definition.
     *
     * @param computation Computation definition for this column.
     */
    public void setComputation(MvComputation computation) {
        this.computation = computation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    @Override
    public String toString() {
        return "MvColumn{" + "name=" + name + '}';
    }

}
