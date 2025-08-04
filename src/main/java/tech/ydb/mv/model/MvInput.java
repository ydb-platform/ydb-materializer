package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvInput implements MvSqlPosHolder {

    private String tableName;
    private String changeFeed;
    private MvTableInfo tableInfo;
    private boolean batchMode;
    private MvSqlPos sqlPos;

    public MvInput(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
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

    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(MvTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public String getChangeFeed() {
        return changeFeed;
    }

    public void setChangeFeed(String changeFeed) {
        this.changeFeed = changeFeed;
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    public void setSqlPos(MvSqlPos inputPosition) {
        this.sqlPos = inputPosition;
    }

}
