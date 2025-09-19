package tech.ydb.mv.data;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Information about the dictionary changes that happened since the last refresh.
 *
 * @author zinal
 */
public class MvDictChanges {

    // fieldName -> row keys where the field is modified
    private final HashMap<String, HashSet<MvKey>> fields = new HashMap<>();

    private MvKey scanPosition;

    public HashMap<String, HashSet<MvKey>> getFields() {
        return fields;
    }

    public MvKey getScanPosition() {
        return scanPosition;
    }

    public void setScanPosition(MvKey scanPosition) {
        this.scanPosition = scanPosition;
    }

    public MvDictChanges updateField(String fieldName, MvKey rowKey) {
        HashSet<MvKey> rowKeys = fields.get(fieldName);
        if (rowKeys == null) {
            rowKeys = new HashSet<>();
            fields.put(fieldName, rowKeys);
        }
        rowKeys.add(rowKey);
        return this;
    }

}
