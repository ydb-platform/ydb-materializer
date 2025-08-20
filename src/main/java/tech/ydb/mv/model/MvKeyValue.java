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
public class MvKeyValue extends MvKeyPrefix {

    private final YdbStruct value;

    public MvKeyValue(YdbStruct value, MvKeyInfo info) {
        super(info, MvKeyValue.buildValue(value, info));
        this.value = value;
    }

    public MvKeyValue(YdbStruct value, MvTableInfo tableInfo) {
        this(value, tableInfo.getKeyInfo());
    }

    public MvKeyValue(String json, MvKeyInfo info) {
        this(YdbStruct.fromJson(json), info);
    }

    public MvKeyValue(ResultSetReader rsr, MvKeyInfo info) {
        this(toYdbStruct(rsr), info);
    }

    public YdbStruct getValue() {
        return value;
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
