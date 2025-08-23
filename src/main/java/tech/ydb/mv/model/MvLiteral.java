package tech.ydb.mv.model;

import java.util.Objects;

/**
 *
 * @author zinal
 */
public class MvLiteral {

    private final String value;
    private final String identity;

    public MvLiteral(String value, String identity) {
        this.value = value;
        this.identity = identity;
    }

    public MvLiteral(String value, int identity) {
        this(value, "c" + String.valueOf(identity));
    }

    public String getValue() {
        return value;
    }

    public String getIdentity() {
        return identity;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.value);
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
        final MvLiteral other = (MvLiteral) obj;
        return Objects.equals(this.value, other.value);
    }

}
