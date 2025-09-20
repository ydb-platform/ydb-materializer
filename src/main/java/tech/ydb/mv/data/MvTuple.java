package tech.ydb.mv.data;

import java.io.Serializable;
import java.util.Arrays;

/**
 *
 * @author zinal
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MvTuple implements Comparable<MvTuple>, Serializable {

    private static final long serialVersionUID = 20250920001L;

    protected final Comparable[] values;

    public MvTuple(Comparable[] values) {
        this.values = values;
    }

    public int size() {
        return values.length;
    }

    public Comparable<?> getValue(int pos) {
        return values[pos];
    }

    @Override
    public int compareTo(MvTuple other) {
        int keyLen = Math.min(this.values.length, other.values.length);
        for (int pos = 0; pos < keyLen; ++pos) {
            if (this.values[pos] == null) {
                if (other.values[pos] == null) {
                    continue;
                }
                return -1;
            } else if (other.values[pos] == null) {
                return 1;
            }
            int cmp = this.values[pos].compareTo(other.values[pos]);
            if (cmp != 0) {
                return cmp;
            }
        }
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
        final MvTuple other = (MvTuple) obj;
        return Arrays.deepEquals(this.values, other.values);
    }

}
