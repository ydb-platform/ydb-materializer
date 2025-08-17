package tech.ydb.mv.util;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 *
 * @author zinal
 */
public class YdbBytes implements Comparable<YdbBytes> {

    private final byte[] value;

    public YdbBytes(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
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

    public static class GsonAdapter implements JsonSerializer<YdbBytes>, JsonDeserializer<YdbBytes> {
        @Override
        public JsonElement serialize(YdbBytes src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(Base64.getUrlEncoder().encodeToString(src.getValue()));
        }

        @Override
        public YdbBytes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return new YdbBytes(Base64.getUrlDecoder().decode(json.getAsString()));
        }
    }

}
