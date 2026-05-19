package tech.ydb.mv.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @author zinal
 */
public interface MvViewOption {

    /**
     * @return option name, as used in the CREATE ASYNC VIEW syntax
     */
    String getName();

    /**
     * @return default option value, when not specified
     */
    Object defaultValue();

    /**
     * Parse the option value
     *
     * @param value Option value
     * @param position Parsing position
     * @return Parsed value, or MvIssue instance if the value is not valid
     */
    Object parse(String value, MvSqlPosHolder position);

    public abstract class BaseOption implements MvViewOption, Serializable {

        private final String name;

        public BaseOption(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + Objects.hashCode(this.name);
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
            final BaseOption other = (BaseOption) obj;
            return Objects.equals(this.name, other.name);
        }
    }

    public class BooleanOption extends BaseOption {

        public BooleanOption(String name) {
            super(name);
        }

        @Override
        public Object defaultValue() {
            return false;
        }

        @Override
        public Object parse(String value, MvSqlPosHolder position) {
            if (value == null) {
                return defaultValue();
            }
            value = value.trim().toLowerCase();
            if (value.length() == 0) {
                return defaultValue();
            }
            if ("1".equals(value) || "y".equals(value) || "t".equals(value)
                    || "yes".equals(value) || "true".equals(value)) {
                return true;
            }
            if ("0".equals(value) || "n".equals(value) || "f".equals(value)
                    || "no".equals(value) || "false".equals(value)) {
                return false;
            }
            return new MvIssue.IllegalBooleanValueError(getName(), value, position);
        }

    }

    public static final BooleanOption SKIP_DELETES = new BooleanOption("SKIP_DELETES");

    public static final Map<String, MvViewOption> ENTRIES = buildEntries();

    private static Map<String, MvViewOption> buildEntries() {
        Map<String, MvViewOption> temp = new HashMap<>();
        temp.put(SKIP_DELETES.getName(), SKIP_DELETES);
        return Collections.unmodifiableMap(temp);
    }

}
