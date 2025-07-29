package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvInput implements MvPositionHolder {

    private String tableName;
    private String changeFeed;
    private MvTableInfo tableInfo;
    private boolean batchMode;
    private MvInputPosition inputPosition;

    public MvInput(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
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
    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

}
