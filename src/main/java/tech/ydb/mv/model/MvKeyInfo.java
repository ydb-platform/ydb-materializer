package tech.ydb.mv.model;

import java.util.Arrays;
import java.util.HashMap;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.TupleType;
import tech.ydb.table.values.Type;

/**
 * Key information metadata.
 *
 * @author zinal
 */
public class MvKeyInfo {

    private final MvTableInfo owner;
    private final String[] names;
    private final Type[] types;
    private final TupleType tupleType;
    private final StructType structType;
    private final int[] structIndex;

    public MvKeyInfo(MvTableInfo ti) {
        this.owner = ti;
        int count = ti.getKey().size();
        this.names = new String[count];
        this.types = new Type[count];
        for (int i=0; i<count; ++i) {
            String name = ti.getKey().get(i);
            Type type = ti.getColumns().get(name);
            if (type==null) {
                throw new IllegalStateException("Missing key field information "
                        + "for `" + name + "` in table `" + ti.getName() + "`");
            }
            this.names[i] = name;
            this.types[i] = type;
        }
        this.structType = makeStructType();
        this.tupleType = makeTupleType();
        this.structIndex = makeStructIndex();
    }

    public MvTableInfo getOwner() {
        return owner;
    }

    public int size() {
        return names.length;
    }

    public String getName(int pos) {
        return names[pos];
    }

    public Type getType(int pos) {
        return types[pos];
    }

    public int getStructIndex(int pos) {
        return structIndex[pos];
    }

    public StructType getStructType() {
        return structType;
    }

    public TupleType getTupleType() {
        return tupleType;
    }

    private TupleType makeTupleType() {
        return TupleType.ofCopy(types);
    }

    private StructType makeStructType() {
        HashMap<String, Type> m = new HashMap<>();
        for (int pos = 0; pos < names.length; ++pos) {
            m.put(names[pos], types[pos]);
        }
        return StructType.of(m);
    }

    private int[] makeStructIndex() {
        if (names.length != structType.getMembersCount()) {
            throw new IllegalStateException();
        }
        int index[] = new int[names.length];
        for (int pos = 0; pos < names.length; ++pos) {
            index[pos] = structType.getMemberIndex(names[pos]);
            if (index[pos] < 0) {
                throw new IllegalStateException();
            }
        }
        return index;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 11 * hash + Arrays.deepHashCode(this.names);
        hash = 11 * hash + Arrays.deepHashCode(this.types);
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
        final MvKeyInfo other = (MvKeyInfo) obj;
        if (!Arrays.deepEquals(this.names, other.names)) {
            return false;
        }
        return Arrays.deepEquals(this.types, other.types);
    }

    @Override
    public String toString() {
        return structType.toString();
    }

}
