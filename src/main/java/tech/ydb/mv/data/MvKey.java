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

    public MvKey(YdbStruct value, MvKeyInfo info) {
        super(info, MvKey.buildValue(value, info));
    }

    public MvKey(YdbStruct value, MvTableInfo tableInfo) {
        this(value, tableInfo.getKeyInfo());
    }

    public MvKey(String json, MvKeyInfo info) {
        this(YdbStruct.fromJson(json), info);
    }

    public MvKey(ResultSetReader rsr, MvKeyInfo info) {
        this(toYdbStruct(rsr, info.size()), info);
    }

    public MvKey(MvKeyInfo info, Comparable[] values) {
        super(info, values);
    }

    public boolean isEmpty() {
        for (Comparable c : values) {
            if (c!=null) {
                return false;
            }
        }
        return true;
    }

    public static Comparable[] buildValue(YdbStruct ys, MvKeyInfo info) {
        Comparable[] output = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            output[pos] = ys.get(info.getName(pos));
        }
        return output;
    }

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

    public static YdbStruct toYdbStruct(ResultSetReader rsr) {
        return toYdbStruct(rsr, -1);
    }

}
