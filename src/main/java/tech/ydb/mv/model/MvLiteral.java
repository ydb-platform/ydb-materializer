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
        if (value==null) {
            value = "";
        } else {
            value = value.trim();
        }
        this.value = value;
        this.identity = identity;
        if ("true".equalsIgnoreCase(value)) {
            this.pojo = true;
        } else if ("false".equalsIgnoreCase(value)) {
            this.pojo = false;
        } else if (value.matches("^[+-]?[1-9][0-9]*$")) {
            this.pojo = (long) Long.parseLong(value);
        } else if (value.startsWith("'") && value.endsWith("'")) {
            this.pojo = value.substring(1, value.length()-1);
        } else if (value.startsWith("'")
                && (value.endsWith("'s") || value.endsWith("'u"))) {
            this.pojo = value.substring(1, value.length()-2);
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

    public String getSafeValue() {
        if (isInteger() || isBoolean()) {
            return value;
        }
        if (value.startsWith("'")
                && (value.endsWith("'u") || value.endsWith("'s"))) {
            return value;
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value + "u";
        }
        return "'" + value + "'u";
    }

    public String getIdentity() {
        return identity;
    }

    public boolean isBoolean() {
        return (pojo instanceof Boolean);
    }

    public boolean isInteger() {
        return (pojo instanceof Long);
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
