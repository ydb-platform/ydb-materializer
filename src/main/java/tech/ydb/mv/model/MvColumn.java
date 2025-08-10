package tech.ydb.mv.model;

import tech.ydb.table.values.Type;

/**
 * Column of the materialized view table.
 * @author mzinal
 */
public class MvColumn implements MvSqlPosHolder {

    private final String name;
    private Type type;
    private String sourceAlias;
    private String sourceColumn;
    private MvJoinSource sourceRef;
    private MvComputation computation;
    private final MvSqlPos sqlPos;

    public MvColumn(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
    }

    public boolean isComputation() {
        return (computation != null);
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getSourceAlias() {
        return sourceAlias;
    }

    public void setSourceAlias(String sourceAlias) {
        this.sourceAlias = sourceAlias;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public MvJoinSource getSourceRef() {
        return sourceRef;
    }

    public void setSourceRef(MvJoinSource sourceRef) {
        this.sourceRef = sourceRef;
    }

    public MvComputation getComputation() {
        return computation;
    }

    public void setComputation(MvComputation computation) {
        this.computation = computation;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

}
