package tech.ydb.mv.model;

import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
public class MvKey {

    public static final Gson GSON = new GsonBuilder().create();

    private final MvKeyInfo info;
    private final Object[] values;

    public MvKey(String json, MvKeyInfo info) {
        this.info = info;
        this.values = new Object[info.size()];
        HashMap<String, Object> m = GSON.fromJson(json, HashMap.class);
        for (int pos = 0; pos < info.size(); ++pos) {
            this.values[pos] = m.get(info.getName(pos));
        }
    }

    public MvKey(ResultSetReader rsr, MvKeyInfo info) {
        this.info = info;
        this.values = new Object[info.size()];
        for (int pos = 0; pos < info.size(); ++pos) {
            int colIndex = rsr.getColumnIndex(info.getName(pos));
            if (colIndex >= 0) {
                this.values[pos] = YdbConv.toPojo(rsr.getColumn(colIndex).getValue());
            }
        }
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
}
