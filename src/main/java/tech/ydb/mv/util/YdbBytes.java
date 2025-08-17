package tech.ydb.mv.util;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 *
 * @author zinal
 */
public class YdbBytes implements Comparable<YdbBytes>, Serializable {
    private static final long serialVersionUID = 20250817001L;

    private final byte[] value;

    public YdbBytes(byte[] value) {
        this.value = value;
    }

    public YdbBytes(String encodedValue) {
        this.value = Base64.getUrlDecoder().decode(encodedValue);
    }

    public byte[] getValue() {
        return value;
    }

    public String encode() {
        return Base64.getUrlEncoder().encodeToString(value);
    }

    @Override
    public int compareTo(YdbBytes o) {
        return Arrays.compareUnsigned(value, o.value);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + Arrays.hashCode(this.value);
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
        final YdbBytes other = (YdbBytes) obj;
        return Arrays.equals(this.value, other.value);
    }

    @Override
    public String toString() {
        try {
            return new String(value, StandardCharsets.UTF_8);
        } catch(Exception e) {
            return Base64.getUrlEncoder().encodeToString(value);
        }
    }

}
