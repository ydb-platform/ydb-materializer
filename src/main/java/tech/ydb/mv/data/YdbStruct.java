package tech.ydb.mv.data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Serializable "struct-like" container for YDB values used by the Materializer.
 *
 * Supports a fixed set of comparable value types and can be serialized to/from
 * JSON for storing change images and keys.
 *
 * @author zinal
 */
public class YdbStruct implements Serializable {

    private static final long serialVersionUID = 20250817001L;

    public static final YdbStruct EMPTY = new YdbStruct(0);

    public static Map<Class<?>, TypeInfo<?>> CLS2INFO;
    public static Map<String, TypeInfo<?>> CODE2INFO;

    static {
        ArrayList<TypeInfo<?>> types = new ArrayList<>();
        types.add(typeInfo(Boolean.class, "a",
                v -> v.toString(),
                v -> Boolean.valueOf(v)));
        types.add(typeInfo(String.class, "b",
                v -> v.toString(),
                v -> v));
        types.add(typeInfo(YdbBytes.class, "c",
                v -> ((YdbBytes) v).encode(),
                v -> new YdbBytes(v)));
        types.add(typeInfo(YdbUnsigned.class, "d",
                v -> v.toString(),
                v -> new YdbUnsigned(v)));
        types.add(typeInfo(Float.class, "e",
                v -> v.toString(),
                v -> Float.valueOf(v)));
        types.add(typeInfo(Double.class, "f",
                v -> v.toString(),
                v -> Double.valueOf(v)));
        types.add(typeInfo(Short.class, "g",
                v -> v.toString(),
                v -> Short.valueOf(v)));
        types.add(typeInfo(Integer.class, "h",
                v -> v.toString(),
                v -> Integer.valueOf(v)));
        types.add(typeInfo(Long.class, "i",
                v -> v.toString(),
                v -> Long.valueOf(v)));
        types.add(typeInfo(BigDecimal.class, "j",
                v -> v.toString(),
                v -> new BigDecimal(v)));
        types.add(typeInfo(LocalDate.class, "k",
                v -> v.toString(),
                v -> LocalDate.parse(v)));
        types.add(typeInfo(LocalDateTime.class, "l",
                v -> ((LocalDateTime) v).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                v -> LocalDateTime.parse(v)));
        types.add(typeInfo(Instant.class, "m",
                v -> v.toString(),
                v -> Instant.parse(v)));
        types.add(typeInfo(Duration.class, "n",
                v -> String.valueOf(((Duration) v).toSeconds()),
                v -> Duration.ofSeconds(Integer.parseInt(v))));

        HashMap<Class<?>, TypeInfo<?>> m0 = new HashMap<>();
        types.forEach(ti -> m0.put(ti.clazz, ti));
        CLS2INFO = Collections.unmodifiableMap(m0);

        HashMap<String, TypeInfo<?>> m1 = new HashMap<>();
        types.forEach(ti -> m1.put(ti.typeCode, ti));
        CODE2INFO = Collections.unmodifiableMap(m1);
    }

    private final Map<String, Comparable<?>> values;

    /**
     * Create an empty, mutable struct.
     */
    public YdbStruct() {
        this.values = new HashMap<>();
    }

    /**
     * Create a struct with expected capacity.
     *
     * If {@code capacity <= 0} the struct is created as empty/immutable.
     *
     * @param capacity Expected number of members.
     */
    public YdbStruct(int capacity) {
        if (capacity > 0) {
            this.values = new HashMap<>(capacity);
        } else {
            this.values = Collections.emptyMap();
        }
    }

    /**
     * @return {@code true} if the struct contains no fields.
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * @return {@code true} if the struct contains at least one field.
     */
    public boolean isFilled() {
        return !values.isEmpty();
    }

    /**
     * Get a set of field names.
     *
     * @return Field names.
     */
    public Set<String> keySet() {
        return values.keySet();
    }

    /**
     * Retrieve a value of a specific field.
     *
     * @param name Name of the field to be retrieved.
     * @return Field value, or null if such a field was not found.
     */
    public Comparable<?> get(String name) {
        return values.get(name);
    }

    /**
     * Put a value into the struct.
     *
     * @param name Field name.
     * @param v Field value (may be {@code null}).
     * @return Previous value.
     */
    public Comparable<?> put(String name, Comparable<?> v) {
        if (v != null) {
            if (CLS2INFO.get(v.getClass()) == null) {
                throw new IllegalArgumentException("Unsupported data type for YdbStruct: "
                        + v.getClass());
            }
        }
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Boolean v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, String v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, YdbBytes v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Float v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Double v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Byte v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Short v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Integer v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Long v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, YdbUnsigned v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, BigDecimal v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, LocalDate v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, LocalDateTime v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Instant v) {
        return values.put(name, v);
    }

    /**
     * See {@link #put(String, Comparable)}.
     */
    public Comparable<?> put(String name, Duration v) {
        return values.put(name, v);
    }

    /**
     * Add a value into the struct (fluent API).
     *
     * @param name Field name.
     * @param v Field value (may be {@code null}).
     * @return This instance.
     */
    public YdbStruct add(String name, Comparable<?> v) {
        if (v != null) {
            if (CLS2INFO.get(v.getClass()) == null) {
                throw new IllegalArgumentException("Unsupported data type for YdbStruct: "
                        + v.getClass());
            }
        }
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Boolean v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, String v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, YdbBytes v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Float v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Double v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Byte v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Short v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Integer v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Long v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, YdbUnsigned v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, BigDecimal v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, LocalDate v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, LocalDateTime v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Instant v) {
        values.put(name, v);
        return this;
    }

    /**
     * See {@link #add(String, Comparable)}.
     */
    public YdbStruct add(String name, Duration v) {
        values.put(name, v);
        return this;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + Objects.hashCode(this.values);
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
        final YdbStruct other = (YdbStruct) obj;
        return Objects.equals(this.values, other.values);
    }

    /**
     * Convert struct to JSON string.
     *
     * @return JSON representation.
     */
    public String toJson() {
        return appendJson(new JsonObject()).toString();
    }

    /**
     * Append this struct as JSON properties to the provided object.
     *
     * @param root Root JSON object.
     * @return The same {@code root} instance.
     */
    public JsonObject appendJson(JsonObject root) {
        for (Map.Entry<String, Comparable<?>> me : values.entrySet()) {
            if (me.getValue() == null) {
                continue; // skipping null values
            }
            JsonObject jme = new JsonObject();
            TypeInfo<?> ti = CLS2INFO.get(me.getValue().getClass());
            if (ti == null) {
                throw new IllegalStateException("Unsupported value of type "
                        + me.getValue().getClass());
            }
            jme.addProperty("t", ti.typeCode);
            jme.addProperty("v", ti.fnOut.apply(me.getValue()));
            root.add(me.getKey(), jme);
        }
        return root;
    }

    @Override
    public String toString() {
        return toJson();
    }

    /**
     * Parse struct from JSON string created by {@link #toJson()}.
     *
     * @param json JSON input.
     * @return Parsed struct.
     */
    public static YdbStruct fromJson(String json) {
        JsonElement root = JsonParser.parseString(json);
        if (!root.isJsonObject()) {
            return new YdbStruct();
        }
        YdbStruct ret = new YdbStruct();
        for (Map.Entry<String, JsonElement> me : root.getAsJsonObject().entrySet()) {
            if (!me.getValue().isJsonObject()) {
                continue;
            }
            JsonObject jme = me.getValue().getAsJsonObject();
            String typeCode = jme.get("t").getAsString();
            String value = jme.get("v").getAsString();
            if (typeCode == null || value == null) {
                continue;
            }
            TypeInfo<?> ti = CODE2INFO.get(typeCode);
            if (ti == null) {
                throw new RuntimeException("Unexpected type code: " + typeCode);
            }
            Comparable<?> parsedValue = ti.fnIn.apply(value);
            if (parsedValue != null) {
                ret.put(me.getKey(), parsedValue);
            }
        }
        return ret;
    }

    @SuppressWarnings("rawtypes")
    private static <T> TypeInfo<T> typeInfo(Class<T> clazz, String typeCode,
            Function<Comparable, String> fnOut,
            Function<String, Comparable> fnIn) {
        return new TypeInfo<>(clazz, typeCode, fnOut, fnIn);
    }

    /**
     * Internal type descriptor used for JSON serialization/deserialization.
     *
     * @param <T> The actual data type used
     */
    @SuppressWarnings("rawtypes")
    public static class TypeInfo<T> {

        public final String typeCode;
        public final Class<T> clazz;
        public final Function<Comparable, String> fnOut;
        public final Function<String, Comparable> fnIn;

        private TypeInfo(Class<T> clazz, String typeCode,
                Function<Comparable, String> fnOut,
                Function<String, Comparable> fnIn) {
            this.typeCode = typeCode;
            this.clazz = clazz;
            this.fnOut = fnOut;
            this.fnIn = fnIn;
        }
    }
}
