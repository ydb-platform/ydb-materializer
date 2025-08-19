package tech.ydb.mv.model;

import tech.ydb.table.result.ResultSetReader;

import tech.ydb.mv.util.YdbConv;
import tech.ydb.mv.util.YdbStruct;

/**
 * Table key in the comparable form.
 *
 * @author zinal
 */
@SuppressWarnings("rawtypes")
public class MvKey extends MvKeyPrefix {

    public MvKey(YdbStruct ys, MvKeyInfo info) {
        super(info, MvKey.buildValue(ys, info));
    }

    public MvKey(YdbStruct ys, MvTableInfo tableInfo) {
        super(tableInfo.getKeyInfo(), MvKey.buildValue(ys, tableInfo.getKeyInfo()));
    }

    public MvKey(String json, MvKeyInfo info) {
        super(info, MvKey.buildValue(YdbStruct.fromJson(json), info));
    }

    public MvKey(ResultSetReader rsr, MvKeyInfo info) {
        super(info, buildValue(rsr, info));
    }

    public static Comparable[] buildValue(YdbStruct ys, MvKeyInfo info) {
        Comparable[] output = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            output[pos] = ys.get(info.getName(pos));
        }
        return output;
    }

    public static Comparable[] buildValue(ResultSetReader rsr, MvKeyInfo info) {
        Comparable[] output = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            int colIndex = rsr.getColumnIndex(info.getName(pos));
            if (colIndex >= 0) {
                output[pos] = YdbConv.toPojo(rsr.getColumn(colIndex).getValue());
            }
        }
        return output;
    }

}
