package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvJoinSource implements MvPositionHolder {

    private String tableName;
    private String alias;
    private Mode mode;
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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
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

    public static enum Mode {
        MAIN,
        INNER,
        LEFT
    }
}
