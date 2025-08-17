package tech.ydb.mv.model;

import java.util.HashMap;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import tech.ydb.mv.util.YdbConv;
import tech.ydb.table.values.TupleValue;

/**
 * Key value in the serializable and convertible form.
 *
 * @author zinal
 */
public class MvKey {

    public static final Gson GSON = new GsonBuilder().create();

    private final StructType keyType;
    private final HashMap<String, Object> keys = new HashMap<>();

    public MvKey(ResultSetReader rsr, StructType keyType) {
        this.keyType = keyType;
        for (int ix = 0; ix < keyType.getMembersCount(); ++ix) {
            String name = keyType.getMemberName(ix);
            int colIndex = rsr.getColumnIndex(name);
            if (colIndex >= 0) {
                keys.put(name, YdbConv.toPojo(rsr.getColumn(colIndex).getValue()));
            }
        }
    }

    public MvKey(String json, StructType keyType) {
        this.keyType = keyType;
        GSON.fromJson(json, HashMap.class)
                .forEach((k, v) -> keys.put(k.toString(), v));
    }

    public StructValue toStructValue() {
        int count = keyType.getMembersCount();
        Value<?>[] members = new Value<?>[count];
        for (int ix = 0; ix < count; ++ix) {
            Object v = keys.get(keyType.getMemberName(ix));
            members[ix] = YdbConv.fromPojo(v, keyType.getMemberType(ix));
        }
        return keyType.newValueUnsafe(members);
    }

    public TupleValue toTupleValue(List<String> keyColumns) {
        int count = keyColumns.size();
        Value<?>[] members = new Value<?>[count];
        for (int ix = 0; ix < count; ++ix) {
            String name = keyColumns.get(ix);
            int typePos = keyType.getMemberIndex(name);
            if (typePos >= 0) {
                Object v = keys.get(name);
                members[ix] = YdbConv.fromPojo(v, keyType.getMemberType(typePos));
            }
        }
        return TupleValue.ofOwn(members);
    }

    public String toJson() {
        return GSON.toJson(keys);
    }
}
