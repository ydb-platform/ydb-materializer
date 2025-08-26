package tech.ydb.mv.model;

import java.util.Objects;
import tech.ydb.mv.util.YdbConv;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

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

    public Value<?> toValue() {
        if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
            return PrimitiveValue.newText(value.substring(1, value.length()-1));
        }
        if (value.matches("[+-]?[1-9][0-9]*")) {
            return PrimitiveValue.newInt64(Long.parseLong(value));
        }
        // Ugly fallback
        return PrimitiveValue.newText(value);
    }

    public Value<?> toValue(Type type) {
        Object o = value;
        if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
            o = value.substring(1, value.length()-1);
        }
        return YdbConv.fromPojo(o, type);
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
