package tech.ydb.mv.data;

import java.io.Serializable;

/**
 * Unsigned 64-bit integer wrapper used by the Materializer for YDB UINT64
 * values.
 *
 * @author zinal
 */
public class YdbUnsigned implements Comparable<YdbUnsigned>, Serializable {

    private static final long serialVersionUID = 20250817001L;

    private final long value;

    /**
     * Create wrapper from raw unsigned long bits.
     *
     * @param value Value stored as unsigned {@code long}.
     */
    public YdbUnsigned(long value) {
        this.value = value;
    }

    /**
     * Create wrapper from an unsigned decimal string representation.
     *
     * @param value Unsigned long in decimal form.
     */
    public YdbUnsigned(String value) {
        this.value = Long.parseUnsignedLong(value);
    }

    public long getValue() {
        return value;
    }

    /**
     * Compare unsigned values.
     *
     * @param o Other value.
     * @return Comparison result.
     */
    @Override
    public int compareTo(YdbUnsigned o) {
        return Long.compareUnsigned(value, o.value);
    }

    @Override
    public String toString() {
        return Long.toUnsignedString(value);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + (int) (this.value ^ (this.value >>> 32));
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
        final YdbUnsigned other = (YdbUnsigned) obj;
        return this.value == other.value;
    }

}
