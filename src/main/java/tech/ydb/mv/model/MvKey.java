package tech.ydb.mv.model;

import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import tech.ydb.mv.util.YdbBytes;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.TupleValue;

import tech.ydb.mv.util.YdbConv;

/**
 * Key value in the serializable and convertible form.
 *
 * @author zinal
 */
public class MvKey implements Comparable<MvKey> {

    public static final Gson GSON = new GsonBuilder()
            .registerTypeHierarchyAdapter(YdbBytes.class, new YdbBytes.GsonAdapter())
            .create();

    private final MvKeyInfo info;
    private final Comparable[] values;

    public MvKey(String json, MvKeyInfo info) {
        this.info = info;
        this.values = new Comparable[info.size()];
        HashMap<String, Comparable> m = GSON.fromJson(json, HashMap.class);
        for (int pos = 0; pos < info.size(); ++pos) {
            this.values[pos] = m.get(info.getName(pos));
        }
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

    public MvKeyInfo getInfo() {
        return info;
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
        HashMap<String, Object> m = new HashMap<>(count);
        for (int pos = 0; pos < count; ++pos) {
            m.put(info.getName(pos), values[pos]);
        }
        return GSON.toJson(m);
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
