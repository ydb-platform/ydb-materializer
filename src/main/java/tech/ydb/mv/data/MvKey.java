package tech.ydb.mv.data;

import tech.ydb.table.result.ResultSetReader;

import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTableInfo;

/**
 * Table key and optional value.
 *
 * @author zinal
 */
@SuppressWarnings("rawtypes")
public class MvKey extends MvKeyPrefix {

    /**
     * Create a key from a struct value and key metadata.
     *
     * @param value Key values as a struct.
     * @param info Key metadata.
     */
    public MvKey(YdbStruct value, MvKeyInfo info) {
        super(info, MvKey.buildValue(value, info));
    }

    /**
     * Create a key from a struct value and table metadata.
     *
     * @param value Key values as a struct.
     * @param tableInfo Table metadata.
     */
    public MvKey(YdbStruct value, MvTableInfo tableInfo) {
        this(value, tableInfo.getKeyInfo());
    }

    /**
     * Create a key from JSON and key metadata.
     *
     * @param json JSON representation of a key.
     * @param info Key metadata.
     */
    public MvKey(String json, MvKeyInfo info) {
        this(YdbStruct.fromJson(json), info);
    }

    /**
     * Create a key from a result set row and key metadata.
     *
     * @param rsr Result set reader positioned at a row.
     * @param info Key metadata.
     */
    public MvKey(ResultSetReader rsr, MvKeyInfo info) {
        this(toYdbStruct(rsr, info.size()), info);
    }

    /**
     * Create a key from raw values and key metadata.
     *
     * @param info Key metadata.
     * @param values Key values.
     */
    public MvKey(MvKeyInfo info, Comparable[] values) {
        super(info, values);
    }

    /**
     * Check whether all key components are {@code null}.
     *
     * @return {@code true} if all values are {@code null}.
     */
    public boolean isEmpty() {
        for (Comparable c : values) {
            if (c != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Build key value array from a struct according to key metadata order.
     *
     * @param ys Struct holding key members by name.
     * @param info Key metadata.
     * @return Key values in the key column order.
     */
    public static Comparable[] buildValue(YdbStruct ys, MvKeyInfo info) {
        Comparable[] output = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            output[pos] = ys.get(info.getName(pos));
        }
        return output;
    }

    /**
     * Convert a result set row to a {@link YdbStruct}.
     *
     * @param rsr Result set reader positioned at a row.
     * @param maxColumns Maximum number of columns to include (negative means
     * all).
     * @return Struct containing column name/value pairs.
     */
    public static YdbStruct toYdbStruct(ResultSetReader rsr, int maxColumns) {
        int count = rsr.getColumnCount();
        if (maxColumns > 0 && count > maxColumns) {
            count = maxColumns;
        }
        YdbStruct ys = new YdbStruct(count);
        for (int pos = 0; pos < count; ++pos) {
            ys.add(rsr.getColumnName(pos), YdbConv.toPojo(rsr.getColumn(pos).getValue()));
        }
        return ys;
    }

    /**
     * Convert a result set row to a {@link YdbStruct}, including all columns.
     *
     * @param rsr Result set reader positioned at a row.
     * @return Struct containing column name/value pairs.
     */
    public static YdbStruct toYdbStruct(ResultSetReader rsr) {
        return toYdbStruct(rsr, -1);
    }

}
