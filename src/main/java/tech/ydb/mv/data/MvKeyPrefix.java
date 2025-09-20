package tech.ydb.mv.data;

import tech.ydb.table.description.KeyBound;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;

import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTableInfo;

/**
 * Key prefix in the comparable form. Includes the link to the key information,
 * which is linked to the specific table.
 *
 * @author zinal
 */
public class MvKeyPrefix extends MvTuple {

    protected final MvKeyInfo info;

    protected MvKeyPrefix(MvKeyInfo info, Comparable[] values) {
        super(values);
        this.info = info;
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

    public String getTableName() {
        return info.getOwner().getName();
    }

    public MvTableInfo getTableInfo() {
        return info.getOwner();
    }

    public String getName(int pos) {
        return info.getName(pos);
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
    public int compareTo(MvTuple other) {
        if (!(other instanceof MvKeyPrefix)) {
            throw new IllegalArgumentException("Refusing to compare "
                    + getClass() + " versus " + other.getClass());
        }
        MvKeyPrefix otherPrefix = (MvKeyPrefix) other;
        if (info != otherPrefix.info
                && !getTableName().equals(otherPrefix.getTableName())) {
            throw new IllegalArgumentException("Refusing to compare keys between "
                    + "`" + getTableName() + "` and `" + otherPrefix.getTableName() + "`");
        }
        return super.compareTo(other);
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
                return makePrefix((TupleValue) value, info);
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
            if (output[pos] == null) { // can reduce tuple until first null
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
            if (ys.get(info.getName(pos)) != null) {
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
