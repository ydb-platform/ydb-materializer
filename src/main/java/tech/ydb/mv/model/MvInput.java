package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Input table configuration for a handler: source table name, changefeed, and references.
 * @author zinal
 */
public class MvInput implements MvSqlPosHolder {

    private final String tableName;
    private final String changefeed;
    private MvTableInfo tableInfo;
    private boolean batchMode;
    private final ArrayList<Reference> references = new ArrayList<>();
    private final MvSqlPos sqlPos;

    /**
     * Create input definition.
     *
     * @param tableName Source table name.
     * @param changefeed Changefeed name (may be {@code null}).
     * @param sqlPos Position in the SQL text.
     */
    public MvInput(String tableName, String changefeed, MvSqlPos sqlPos) {
        this.tableName = tableName;
        this.changefeed = changefeed;
        this.sqlPos = sqlPos;
    }

    /**
     * Check whether table info was resolved and matches the configured name.
     *
     * @return {@code true} if {@link #getTableInfo()} is set and matches {@link #getTableName()}.
     */
    public boolean isTableKnown() {
        return (tableInfo != null) && (tableName != null)
                && tableName.equals(tableInfo.getName());
    }

    /**
     * Get resolved changefeed metadata for this input.
     *
     * @return Changefeed info, or {@code null} if not available.
     */
    public MvTableInfo.Changefeed getChangefeedInfo() {
        if (tableInfo == null || changefeed == null) {
            return null;
        }
        return tableInfo.getChangefeeds().get(changefeed);
    }

    /**
     * Get source table name.
     *
     * @return Source table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get resolved table info.
     *
     * @return Resolved table info (may be {@code null} until described).
     */
    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    /**
     * Set resolved table info.
     *
     * @param tableInfo Resolved table info.
     */
    public void setTableInfo(MvTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    /**
     * Get changefeed name.
     *
     * @return Changefeed name (may be {@code null}).
     */
    public String getChangefeed() {
        return changefeed;
    }

    /**
     * Check whether this input uses batch mode.
     *
     * @return {@code true} if this input should be treated in batch/scan mode.
     */
    public boolean isBatchMode() {
        return batchMode;
    }

    /**
     * Set whether this input uses batch mode.
     *
     * @param batchMode Whether this input should be treated in batch/scan mode.
     */
    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    /**
     * Get references from view parts to join sources that use this input.
     *
     * @return Unmodifiable list of references.
     */
    public List<Reference> getReferences() {
        return Collections.unmodifiableList(references);
    }

    /**
     * Add a reference to this input.
     *
     * @param part View part where the reference occurs.
     * @param js Join source referring to this input.
     */
    public void addReference(MvViewExpr part, MvJoinSource js) {
        references.add(new Reference(part, js));
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    /**
     * Link between a view part and a join source referencing this input.
     */
    public static class Reference {

        private final MvViewExpr part;
        private final MvJoinSource source;

        /**
         * Create a reference.
         *
         * @param part View part where the reference occurs.
         * @param source Join source referring to the input.
         */
        public Reference(MvViewExpr part, MvJoinSource source) {
            this.part = part;
            this.source = source;
        }

        /**
         * Get view part.
         *
         * @return View part where the reference occurs.
         */
        public MvViewExpr getPart() {
            return part;
        }

        /**
         * Get join source.
         *
         * @return Join source that refers to the input.
         */
        public MvJoinSource getSource() {
            return source;
        }
    }

}
