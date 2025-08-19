package tech.ydb.mv.model;

/**
 *
 * @author zinal
 */
public class MvInput implements MvSqlPosHolder {

    private final String tableName;
    private final String changeFeed;
    private MvTableInfo tableInfo;
    private boolean batchMode;
    private String consumerName;
    private final MvSqlPos sqlPos;

    public MvInput(String tableName, String changeFeed, MvSqlPos sqlPos) {
        this.tableName = tableName;
        this.changeFeed = changeFeed;
        this.sqlPos = sqlPos;
    }

    public boolean isTableKnown() {
        return (tableInfo!=null) && (tableName!=null)
                && tableName.equals(tableInfo.getName());
    }

    public String getTableName() {
        return tableName;
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

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

}
