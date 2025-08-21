package tech.ydb.mv.model;

import tech.ydb.table.result.ResultSetReader;

import tech.ydb.mv.util.YdbConv;
import tech.ydb.mv.util.YdbStruct;

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
        this(toYdbStruct(rsr), info);
    }

    public static Comparable[] buildValue(YdbStruct ys, MvKeyInfo info) {
        Comparable[] output = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            output[pos] = ys.get(info.getName(pos));
        }
        return output;
    }

    public static YdbStruct toYdbStruct(ResultSetReader rsr) {
        int count = rsr.getColumnCount();
        YdbStruct ys = new YdbStruct(count);
        for (int pos = 0; pos < count; ++pos) {
            ys.add(rsr.getColumnName(pos), YdbConv.toPojo(rsr.getColumn(pos).getValue()));
        }
        return ys;
    }

}
