package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author zinal
 */
public class MvInput implements MvSqlPosHolder {

    private final String tableName;
    private final String changefeed;
    private MvTableInfo tableInfo;
    private boolean batchMode;
    private final ArrayList<Reference> references = new ArrayList<>();
    private final MvSqlPos sqlPos;

    public MvInput(String tableName, String changefeed, MvSqlPos sqlPos) {
        this.tableName = tableName;
        this.changefeed = changefeed;
        this.sqlPos = sqlPos;
    }

    public boolean isTableKnown() {
        return (tableInfo != null) && (tableName != null)
                && tableName.equals(tableInfo.getName());
    }

    public MvTableInfo.Changefeed getChangefeedInfo() {
        if (tableInfo == null || changefeed == null) {
            return null;
        }
        return tableInfo.getChangefeeds().get(changefeed);
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

    public String getChangefeed() {
        return changefeed;
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    public List<Reference> getReferences() {
        return Collections.unmodifiableList(references);
    }

    public void addReference(MvViewExpr part, MvJoinSource js) {
        references.add(new Reference(part, js));
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    public static class Reference {

        private final MvViewExpr part;
        private final MvJoinSource source;

        public Reference(MvViewExpr part, MvJoinSource source) {
            this.part = part;
            this.source = source;
        }

        public MvViewExpr getPart() {
            return part;
        }

        public MvJoinSource getSource() {
            return source;
        }
    }

}
