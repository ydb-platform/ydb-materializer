package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvInput {

    private String tableName;
    private MvTableRef tableRef;
    private String changeFeed;
    private MvInputPosition inputPosition;

    public MvInput(String tableName, String changeFeed) {
        this.tableName = tableName;
        this.changeFeed = changeFeed;
    }

    public MvInput(MvTableRef tableRef, String changeFeed) {
        this.tableRef = tableRef;
        this.tableName = tableRef.getReference();
        this.changeFeed = changeFeed;
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

    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

}
