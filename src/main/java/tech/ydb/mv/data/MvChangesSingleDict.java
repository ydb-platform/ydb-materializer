package tech.ydb.mv.data;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Information about the dictionary changes that happened since the last
 * refresh.
 *
 * @author zinal
 */
public class MvChangesSingleDict {

    // dictionary table name
    private final String tableName;
    // fieldName -> row keys where the field is modified
    private final HashMap<String, HashSet<MvKey>> fields = new HashMap<>();
    // the last key in the dictionary log
    private MvKey scanPosition;
    // whether the diff field has missing values (e.g. skipped rows)
    private boolean missingDiffFieldRows = false;

    /**
     * Create change tracking for a single dictionary table.
     *
     * @param tableName Dictionary table name.
     */
    public MvChangesSingleDict(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get dictionary table name.
     *
     * @return Dictionary table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get map of changed fields to the affected row keys.
     *
     * @return Field name to set of row keys where that field changed.
     */
    public HashMap<String, HashSet<MvKey>> getFields() {
        return fields;
    }

    /**
     * Get last scan position.
     *
     * @return Last scan position key in the dictionary log (may be {@code null}).
     */
    public MvKey getScanPosition() {
        return scanPosition;
    }

    /**
     * Set last scan position.
     *
     * @param scanPosition Last scan position key in the dictionary log.
     */
    public void setScanPosition(MvKey scanPosition) {
        this.scanPosition = scanPosition;
    }

    /**
     * Record a change for a given field on a given row.
     *
     * @param fieldName Field name that changed.
     * @param rowKey Row key where the field changed.
     * @return This instance for chaining.
     */
    public MvChangesSingleDict updateField(String fieldName, MvKey rowKey) {
        HashSet<MvKey> rowKeys = fields.get(fieldName);
        if (rowKeys == null) {
            rowKeys = new HashSet<>();
            fields.put(fieldName, rowKeys);
        }
        rowKeys.add(rowKey);
        return this;
    }

    /**
     * Check whether any changes were recorded.
     *
     * @return {@code true} if no field changes were recorded.
     */
    public boolean isEmpty() {
        for (var v : fields.values()) {
            if (!v.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether diffs had missing rows.
     *
     * @return {@code true} if CDC diff contained missing values (skipped rows).
     */
    public boolean isMissingDiffFieldRows() {
        return missingDiffFieldRows;
    }

    /**
     * Set the "missing diff rows" flag.
     *
     * @param missingDiffFieldRows Whether CDC diff contained missing values (skipped rows).
     */
    public void setMissingDiffFieldRows(boolean missingDiffFieldRows) {
        this.missingDiffFieldRows = missingDiffFieldRows;
    }

}
