package tech.ydb.mv.model;

import tech.ydb.table.values.Type;

/**
 * Column of the materialized view table.
 * @author mzinal
 */
public class MvColumn implements MvSqlPosHolder {

    private String name;
    private Type type;
    private String sourceAlias;
    private String sourceColumn;
    private MvJoinSource sourceRef;
    private MvComputation computation;
    private MvSqlPos sqlPos;

    public MvColumn(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
    }

    public boolean isComputation() {
        return (computation != null);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public void setSqlPos(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
    }

}
