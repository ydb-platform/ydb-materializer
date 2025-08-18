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
        super(info, makePrefix(ys, info));
    }

    public MvKey(String json, MvKeyInfo info) {
        super(info, makePrefix(YdbStruct.fromJson(json), info));
    }

    public MvKey(ResultSetReader rsr, MvKeyInfo info) {
        super(info, makePrefix(rsr, info));
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MvKeyPrefix other) {
        if (! this.info.equals(other.info)) {
            throw new IllegalArgumentException("Cannot compare keys of type "
                    + this.info + " with " + other.info);
        }
        if (this.values.length != other.values.length) {
            throw new IllegalArgumentException("Invalid key length, expected "
                    + this.values.length + ", got " + other.values.length);
        }
        for (int pos = 0; pos < this.values.length; ++pos) {
            if (this.values[pos]==null) {
                if (other.values[pos]==null) {
                    continue;
                }
                return -1;
            } else if (other.values[pos]==null) {
                return 1;
            }
            int cmp = this.values[pos].compareTo(other.values[pos]);
            if (cmp!=0) {
                return cmp;
            }
        }
        return 0;
    }

    public static Comparable[] makePrefix(YdbStruct ys, MvKeyInfo info) {
        Comparable[] output = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            output[pos] = ys.get(info.getName(pos));
        }
        return output;
    }

    public static Comparable[] makePrefix(ResultSetReader rsr, MvKeyInfo info) {
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
