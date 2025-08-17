package tech.ydb.mv.model;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.TupleValue;

import tech.ydb.mv.util.YdbConv;
import tech.ydb.mv.util.YdbStruct;

/**
 * Key value in the serializable and convertible form.
 *
 * @author zinal
 */
@SuppressWarnings("rawtypes")
public class MvKey implements Comparable<MvKey> {

    protected final MvKeyInfo info;
    protected final Comparable[] values;

    public MvKey(YdbStruct ys, MvKeyInfo info) {
        this.info = info;
        this.values = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            this.values[pos] = ys.get(info.getName(pos));
        }
    }

    public MvKey(String json, MvKeyInfo info) {
        this(YdbStruct.fromJson(json), info);
    }

    public MvKey(ResultSetReader rsr, MvKeyInfo info) {
        this.info = info;
        this.values = new Comparable[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            int colIndex = rsr.getColumnIndex(info.getName(pos));
            if (colIndex >= 0) {
                this.values[pos] = YdbConv.toPojo(rsr.getColumn(colIndex).getValue());
            }
        }
    }

    protected MvKey(MvKeyInfo info, Comparable[] values) {
        this.info = info;
        this.values = values;
    }

    public MvKeyInfo getInfo() {
        return info;
    }

    public int size() {
        return values.length;
    }

    public Comparable<?> getValue(int pos) {
        return values[pos];
    }

    public StructValue toStructValue() {
        int count = info.size();
        Value<?>[] members = new Value<?>[count];
        for (int pos = 0; pos < count; ++pos) {
            int structPos = info.getStructIndex(pos);
            members[structPos] = YdbConv.fromPojo(values[pos], info.getType(pos));
        }
        return info.getStructType().newValueUnsafe(members);
    }

    public TupleValue toTupleValue() {
        int count = info.size();
        Value<?>[] members = new Value<?>[count];
        for (int pos = 0; pos < count; ++pos) {
            members[pos] = YdbConv.fromPojo(values[pos], info.getType(pos));
        }
        return info.getTupleType().newValueOwn(members);
    }

    public String toJson() {
        int count = info.size();
        YdbStruct ys = new YdbStruct(count);
        for (int pos = 0; pos < count; ++pos) {
            ys.put(info.getName(pos), values[pos]);
        }
        return ys.toJson();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MvKey other) {
        if (! this.getClass().equals(other.getClass())) {
            throw new IllegalArgumentException("Cannot compare instance of type "
                    + this.getClass() + " with " + other.getClass());
        }
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
}
