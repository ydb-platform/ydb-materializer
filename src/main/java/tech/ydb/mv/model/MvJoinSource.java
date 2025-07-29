package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvJoinSource implements MvPositionHolder {

    private String tableName;
    private String tableAlias;
    private MvJoinMode mode;
    private final ArrayList<MvJoinCondition> conditions = new ArrayList<>();
    private MvTableInfo tableInfo;
    private MvInputPosition inputPosition;

    public MvJoinSource(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
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

    @Override
    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

}
