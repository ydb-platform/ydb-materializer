package tech.ydb.mv.model;

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

}
