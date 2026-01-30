package tech.ydb.mv.feeder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.math.BigDecimal;
import java.util.Map;

import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

import tech.ydb.mv.data.YdbBytes;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.YdbStruct;
import tech.ydb.mv.data.YdbUnsigned;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTableInfo;

/**
 *
 * @author zinal
 */
class MvCdcParser {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcParser.class);

    private final MvTableInfo tableInfo;

    public MvCdcParser(MvInput input) {
        this.tableInfo = input.getTableInfo();
    }

    public ParseResult parse(byte[] jsonData, Instant tv) {
        String jsonText = new String(jsonData, StandardCharsets.UTF_8);
        JsonElement rootElement = JsonParser.parseString(jsonText);
        JsonObject root = rootElement.isJsonObject()
                ? rootElement.getAsJsonObject() : null;
        if ((root == null) || !root.has("key")) {
            LOG.error("unsupported cdc message {}", jsonText);
            return ParseResult.error();
        }
        try {
            JsonArray key = root.get("key").getAsJsonArray();
            JsonElement erase = root.get("erase");
            JsonElement update = root.get("update");
            JsonObject oldImage = getObjectIf(root, "oldImage");
            JsonObject newImage = getObjectIf(root, "newImage");
            boolean updateMode = false;
            if (newImage == null && update != null && update.isJsonObject()) {
                newImage = update.getAsJsonObject();
                updateMode = true;
            }
            MvKey theKey = parseKey(key);
            return ParseResult.success(new MvChangeRecord(
                    theKey, tv,
                    (erase == null) ? MvChangeRecord.OpType.UPSERT : MvChangeRecord.OpType.DELETE,
                    parseImage(updateMode ? null : theKey, oldImage),
                    parseImage(updateMode ? null : theKey, newImage)
            ));
        } catch (Exception ex) {
            LOG.error("error parsing cdc message {}", jsonText, ex);
            return ParseResult.error();
        }
    }

    private static JsonObject getObjectIf(JsonObject parent, String name) {
        if (parent == null) {
            return null;
        }
        JsonElement e = parent.get(name);
        if (e == null) {
            return null;
        }
        return e.getAsJsonObject();
    }

    @SuppressWarnings("rawtypes")
    private MvKey parseKey(JsonArray key) {
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();
        Comparable[] keyValues = new Comparable[keyInfo.size()];
        for (int pos = 0; pos < keyInfo.size(); ++pos) {
            if (pos < key.size()) {
                keyValues[pos] = readValue(key.get(pos), keyInfo.getType(pos));
            }
        }
        return new MvKey(keyInfo, keyValues);
    }

    private YdbStruct parseImage(MvKey theKey, JsonObject image) {
        if (image == null || image.isEmpty()) {
            return YdbStruct.EMPTY;
        }
        YdbStruct ret = new YdbStruct(tableInfo.getColumns().size());
        for (Map.Entry<String, Type> me : tableInfo.getColumns().entrySet()) {
            ret.put(me.getKey(), readValue(image.get(me.getKey()), me.getValue()));
        }
        if (theKey != null) {
            for (int pos = 0; pos < theKey.size(); ++pos) {
                ret.put(theKey.getName(pos), theKey.getValue(pos));
            }
        }
        return ret;
    }

    @SuppressWarnings("rawtypes")
    public static Comparable readValue(JsonElement node, Type type) {
        if (node == null || node.isJsonNull()) {
            return null;
        }

        if (type.getKind() == Type.Kind.OPTIONAL) {
            OptionalType optional = (OptionalType) type;
            return readValue(node, optional.getItemType());
        }

        if (type.getKind() == Type.Kind.DECIMAL) {
            return new BigDecimal(node.getAsString());
        }

        if (type.getKind() == Type.Kind.PRIMITIVE) {
            PrimitiveType primitive = (PrimitiveType) type;
            switch (primitive) {
                case Bool:
                    return node.getAsBoolean();
                case Int8:
                    return (byte) node.getAsInt();
                case Int16:
                    return (short) node.getAsInt();
                case Int32:
                    return node.getAsInt();
                case Int64:
                    return node.getAsLong();
                case Uint8:
                    return node.getAsInt();
                case Uint16:
                    return node.getAsInt();
                case Uint32:
                    return node.getAsLong();
                case Uint64:
                    return new YdbUnsigned(node.getAsLong());
                case Float:
                    return Double.valueOf(node.getAsDouble()).floatValue();
                case Double:
                    return node.getAsDouble();
                case Text:
                    return node.getAsString();
                case Bytes:
                    return new YdbBytes(Base64.getDecoder().decode(node.getAsString()));
                case Yson:
                    LOG.warn("type YSON is not supported, ignored value {}", node.toString());
                    return new YdbBytes("{}".getBytes());
                case Json:
                    return node.toString();
                case JsonDocument:
                    return node.toString();
                case Uuid:
                    return node.getAsString();
                case Date:
                    return Instant.parse(node.getAsString()).atOffset(ZoneOffset.UTC).toLocalDate();
                case Datetime:
                    return Instant.parse(node.getAsString()).atOffset(ZoneOffset.UTC).toLocalDateTime();
                case Timestamp:
                    return Instant.parse(node.getAsString());
                case Interval:
                    return Duration.ofSeconds(node.getAsLong());
                case Date32:
                    return Instant.parse(node.getAsString()).atOffset(ZoneOffset.UTC).toLocalDate();
                case Datetime64:
                    return Instant.parse(node.getAsString()).atOffset(ZoneOffset.UTC).toLocalDateTime();
                case Timestamp64:
                    return Instant.parse(node.getAsString());
                case Interval64:
                    return Duration.ofSeconds(node.getAsLong());
                default:
                    break;
            }
        }

        LOG.warn("unsupported type {}", type);
        throw new RuntimeException("Can't read node value " + node + " with type " + type);
    }

    static class ParseResult {

        private final MvChangeRecord record;
        private final boolean error;

        private ParseResult(MvChangeRecord record, boolean error) {
            this.record = record;
            this.error = error;
        }

        public static ParseResult success(MvChangeRecord record) {
            return new ParseResult(record, false);
        }

        public static ParseResult error() {
            return new ParseResult(null, true);
        }

        public MvChangeRecord getRecord() {
            return record;
        }

        public boolean isError() {
            return error;
        }
    }

}
