package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author zinal
 */
public class MvJoinSource implements MvSqlPosHolder {

    private String tableName;
    private String tableAlias;
    private MvJoinMode mode;
    private final ArrayList<MvJoinCondition> conditions = new ArrayList<>();
    private MvTableInfo tableInfo;
    private MvInput input;
    private final MvSqlPos sqlPos;

    public MvJoinSource(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
    }

    public MvJoinSource() {
        this(MvSqlPos.EMPTY);
    }

    public boolean isTableKnown() {
        return (tableInfo!=null) && (tableName!=null)
                && tableName.equals(tableInfo.getName());
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String alias) {
        this.tableAlias = alias;
    }

    public MvJoinMode getMode() {
        return mode;
    }

    public void setMode(MvJoinMode mode) {
        this.mode = mode;
    }

    public ArrayList<MvJoinCondition> getConditions() {
        return conditions;
    }

    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(MvTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public MvInput getInput() {
        return input;
    }

    public void setInput(MvInput input) {
        this.input = input;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

}
