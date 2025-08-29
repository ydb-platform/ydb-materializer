package tech.ydb.mv.model;

import java.util.Objects;

/**
 *
 * @author zinal
 */
public class MvLiteral {

    private final String value;
    private final String identity;
    private final Comparable<?> pojo;

    public MvLiteral(String value, String identity) {
        this.value = value;
        this.identity = identity;
        if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
            this.pojo = value.substring(1, value.length()-1);
        } else if (value.matches("[+-]?[1-9][0-9]*")) {
            this.pojo = (long) Long.parseLong(value);
        } else {
            this.pojo = value;
        }
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

    public Comparable<?> getPojo() {
        return pojo;
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
