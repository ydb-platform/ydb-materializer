package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvInput implements MvPositionHolder {

    private String tableName;
    private MvTableRef tableRef;
    private String changeFeed;
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

    public MvTableRef getTableRef() {
        return tableRef;
    }

    public void setTableRef(MvTableRef tableRef) {
        this.tableRef = tableRef;
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
