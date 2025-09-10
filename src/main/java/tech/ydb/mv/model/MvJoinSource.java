package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.List;

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

    public MvTableInfo.Changefeed getChangefeedInfo() {
        if (input==null) {
            return null;
        }
        return input.getChangefeedInfo();
    }

    /**
     * Collect all columns used in join conditions for this source.
     *
     * @return List of right-part columns used in join conditions.
     */
    public List<String> collectRightJoinColumns() {
        List<String> joinColumns = new ArrayList<>();
        for (MvJoinCondition cond : conditions) {
            // Check if this condition references the current source and collect the column
            if (tableAlias.equals(cond.getFirstAlias()) && cond.getFirstColumn() != null) {
                if (!joinColumns.contains(cond.getFirstColumn())) {
                    joinColumns.add(cond.getFirstColumn());
                }
            } else if (tableAlias.equals(cond.getSecondAlias()) && cond.getSecondColumn() != null) {
                if (!joinColumns.contains(cond.getSecondColumn())) {
                    joinColumns.add(cond.getSecondColumn());
                }
            }
        }
        return joinColumns;
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

    @Override
    public String toString() {
        return "MvJoinSource{" + "tableName=" + tableName + ", tableAlias=" + tableAlias + '}';
    }

}
