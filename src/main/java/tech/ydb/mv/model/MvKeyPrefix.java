package tech.ydb.mv.model;

import java.util.Arrays;
import java.util.Objects;
import tech.ydb.table.description.KeyBound;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

import tech.ydb.mv.data.YdbConv;
import tech.ydb.mv.data.YdbStruct;
import tech.ydb.table.values.StructValue;

/**
 * Key prefix in the comparable form.
 * Includes the link to the key information, which is linked to the specific table.
 *
 * @author zinal
 */
@SuppressWarnings("rawtypes")
public class MvKeyPrefix implements Comparable<MvKeyPrefix> {

    protected final MvKeyInfo info;
    protected final Comparable[] values;

    protected MvKeyPrefix(MvKeyInfo info, Comparable[] values) {
        this.info = info;
        this.values = values;
    }

    public MvKeyPrefix(KeyBound kb, MvKeyInfo info) {
        this(info, makePrefix(kb, info));
    }

    public MvKeyPrefix(String json, MvKeyInfo info) {
        this(info, makePrefix(json, info));
    }

    public MvKeyPrefix(YdbStruct ys, MvKeyInfo info) {
        this(info, makePrefix(ys, info));
    }

    public MvKeyPrefix(YdbStruct ys, MvTableInfo tableInfo) {
        this(tableInfo.getKeyInfo(), makePrefix(ys, tableInfo.getKeyInfo()));
    }

    public MvKeyInfo getInfo() {
        return info;
    }

    public MvTableInfo getTableInfo() {
        return info.getOwner();
    }

    public int size() {
        return values.length;
    }

    public String getName(int pos) {
        return info.getName(pos);
    }

    public Comparable<?> getValue(int pos) {
        return values[pos];
    }

    public Comparable<?> getValue(String name) {
        int pos = info.getPosition(name);
        if (pos < 0) {
            return null;
        }
        return values[pos];
    }

    public Value<?> convertValue(int pos) {
        return YdbConv.fromPojo(values[pos], info.getType(pos));
    }

    public StructValue convertKeyToStructValue() {
        int count = info.size();
        Value<?>[] members = new Value<?>[count];
        for (int pos = 0; pos < count; ++pos) {
            int structPos = info.getStructIndex(pos);
            members[structPos] = YdbConv.fromPojo(values[pos], info.getType(pos));
        }
        return info.getStructType().newValueUnsafe(members);
    }

    public TupleValue convertKeyToTupleValue() {
        int count = info.size();
        Value<?>[] members = new Value<?>[count];
        for (int pos = 0; pos < count; ++pos) {
            members[pos] = YdbConv.fromPojo(values[pos], info.getType(pos));
        }
        return info.getTupleType().newValueOwn(members);
    }

    public String convertKeyToJson() {
        int count = info.size();
        YdbStruct ys = new YdbStruct(count);
        for (int pos = 0; pos < count; ++pos) {
            ys.put(info.getName(pos), values[pos]);
        }
        return ys.toJson();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(MvKeyPrefix other) {
        if (! this.info.equals(other.info)) {
            throw new IllegalArgumentException("Cannot compare keys of type "
                    + this.info + " with " + other.info);
        }
        int keyLen = Math.min(this.values.length, other.values.length);
        for (int pos = 0; pos < keyLen; ++pos) {
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
        // If the prefix matches, we treat values as equal.
        return 0;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 79 * hash + Arrays.deepHashCode(this.values);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvKeyPrefix other = (MvKeyPrefix) obj;
        if (!Objects.equals(this.info, other.info)) {
            return false;
        }
        return Arrays.deepEquals(this.values, other.values);
    }

    public static Comparable[] makePrefix(KeyBound kb, MvKeyInfo info) {
        return makePrefix(kb.getValue(), info);
    }

    public static Comparable[] makePrefix(Value<?> value, MvKeyInfo info) {
        switch (value.getType().getKind()) {
            case OPTIONAL:
                if (value.asOptional().isPresent()) {
                    return makePrefix(value.asOptional().get(), info);
                }
                return new Comparable[0];
            case TUPLE:
                return makePrefix((TupleValue)value, info);
            case DECIMAL:
            case PRIMITIVE: {
                Comparable[] output = new Comparable[1];
                output[0] = YdbConv.toPojo(value);
                if (output[0] != null) {
                    return output;
                }
                return new Comparable[0];
            }
            default:
                throw new IllegalArgumentException("Unsupported value type for the prefix: "
                        + value.getType());
        }
    }

    public static Comparable[] makePrefix(TupleValue value, MvKeyInfo info) {
        int prefixLen = Math.min(value.size(), info.size());
        Comparable[] output = new Comparable[prefixLen];
        for (int pos = 0; pos < prefixLen; ++pos) {
            output[pos] = YdbConv.toPojo(value.get(pos));
            if (output[pos]==null) { // can reduce tuple until first null
                Comparable[] reduced = new Comparable[pos];
                System.arraycopy(output, 0, reduced, 0, pos);
                return reduced;
            }
        }
        return output;
    }

    public static Comparable[] makePrefix(String json, MvKeyInfo info) {
        return makePrefix(YdbStruct.fromJson(json), info);
    }

    public static Comparable[] makePrefix(YdbStruct ys, MvKeyInfo info) {
        int prefixLen = 0;
        for (int pos = 0; pos < info.size(); ++pos) {
            if ( ys.get(info.getName(pos)) != null ) {
                prefixLen += 1;
            } else {
                break;
            }
        }
        Comparable[] ret = new Comparable[prefixLen];
        for (int pos = 0; pos < prefixLen; ++pos) {
            ret[pos] = ys.get(info.getName(pos));
        }
        return ret;
    }

}
