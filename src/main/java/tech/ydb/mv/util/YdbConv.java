package tech.ydb.mv.util;

import java.math.BigDecimal;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.NullValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * YDB data types conversion functions.
 *
 * @author zinal
 */
public abstract class YdbConv {

    private YdbConv() {}

    public static OptionalValue makeEmpty(Type t) {
        switch (t.getKind()) {
            case OPTIONAL:
                return ((OptionalType)t).emptyValue();
            default:
                return t.makeOptional().emptyValue();
        }
    }

    public static Value<?> fromPojo(Object v, Type t) {
        if (v==null) {
            return makeEmpty(t);
        }
        switch (t.getKind()) {
            case OPTIONAL:
                return fromPojo(v, t.unwrapOptional()).makeOptional();
            case DECIMAL:
                return decimalFromPojo(v, (DecimalType)t);
            case PRIMITIVE:
                return fromPojo(v, (PrimitiveType)t);
            case NULL:
                return NullValue.of();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + t);
        }
    }

    public static Value<?> fromPojo(Object v, PrimitiveType t) {
        if (v==null) {
            return makeEmpty(t);
        }
        switch (t) {
            case Bool:
                return boolFromPojo(v);
            case Bytes:
                return bytesFromPojo(v);
            case Date:
                return dateFromPojo(v);
            case Date32:
                return date32FromPojo(v);
            case Datetime:
                return datetimeFromPojo(v);
            case Datetime64:
                return datetimeFromPojo(v);
            case Double:
                return doubleFromPojo(v);
            case Float:
                return floatFromPojo(v);
            case Int16:
                return int16FromPojo(v);
            case Int32:
                return int32FromPojo(v);
            case Int64:
                return int64FromPojo(v);
            case Int8:
                return int8FromPojo(v);
            case Interval:
                return intervalFromPojo(v);
            case Interval64:
                return interval64FromPojo(v);
            case Json:
                return jsonFromPojo(v);
            case JsonDocument:
                return jsonDocumentFromPojo(v);
            case Text:
                return textFromPojo(v);
            case Timestamp:
                return timestampFromPojo(v);
            case Timestamp64:
                return timestamp64FromPojo(v);
            case Uint16:
                return uint16FromPojo(v);
            case Uint32:
                return uint32FromPojo(v);
            case Uint64:
                return uint64FromPojo(v);
            case Uint8:
                return uint8FromPojo(v);
            case Uuid:
                return uuidFromPojo(v);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + t);
        }
    }

    private static Value<?> decimalFromPojo(Object v, DecimalType dt) {
        if (v==null) {
            return makeEmpty(dt);
        }
        if (v instanceof BigDecimal) {
            return dt.newValue((BigDecimal) v);
        }
        if (v instanceof Integer) {
            return dt.newValue(((Integer)v));
        }
        if (v instanceof Long) {
            return dt.newValue(((Long)v));
        }
        return dt.newValue(v.toString());
    }

    private static Value<?> boolFromPojo(Object v) {
        if (v instanceof Boolean) {
            return PrimitiveValue.newBool((Boolean) v);
        }
        return PrimitiveValue.newBool(Boolean.parseBoolean(v.toString()));
    }

    private static Value<?> bytesFromPojo(Object v) {
        if (v instanceof YdbBytes) {
            return PrimitiveValue.newBytes(((YdbBytes) v).getValue());
        }
        if (v instanceof byte[]) {
            return PrimitiveValue.newBytes((byte[]) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newBytes(new YdbBytes((String) v).getValue());
        }
        return PrimitiveValue.newBytes(v.toString().getBytes());
    }

    private static Value<?> dateFromPojo(Object v) {
        if (v instanceof java.time.LocalDate) {
            return PrimitiveValue.newDate((java.time.LocalDate) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newDate(java.time.LocalDate.parse((String) v));
        }
        return PrimitiveValue.newDate(java.time.LocalDate.parse(v.toString()));
    }

    private static Value<?> date32FromPojo(Object v) {
        if (v instanceof java.time.LocalDate) {
            return PrimitiveValue.newDate32((java.time.LocalDate) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newDate32(java.time.LocalDate.parse((String) v));
        }
        return PrimitiveValue.newDate32(java.time.LocalDate.parse(v.toString()));
    }

    private static Value<?> datetimeFromPojo(Object v) {
        if (v instanceof java.time.LocalDateTime) {
            return PrimitiveValue.newDatetime((java.time.LocalDateTime) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newDatetime(java.time.LocalDateTime.parse((String) v));
        }
        return PrimitiveValue.newDatetime(java.time.LocalDateTime.parse(v.toString()));
    }

    private static Value<?> doubleFromPojo(Object v) {
        if (v instanceof Double) {
            return PrimitiveValue.newDouble((Double) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newDouble(((Number) v).doubleValue());
        }
        return PrimitiveValue.newDouble(Double.parseDouble(v.toString()));
    }

    private static Value<?> floatFromPojo(Object v) {
        if (v instanceof Float) {
            return PrimitiveValue.newFloat((Float) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newFloat(((Number) v).floatValue());
        }
        return PrimitiveValue.newFloat(Float.parseFloat(v.toString()));
    }

    private static Value<?> int16FromPojo(Object v) {
        if (v instanceof Short) {
            return PrimitiveValue.newInt16((Short) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt16(((Number) v).shortValue());
        }
        return PrimitiveValue.newInt16(Short.parseShort(v.toString()));
    }

    private static Value<?> int32FromPojo(Object v) {
        if (v instanceof Integer) {
            return PrimitiveValue.newInt32((Integer) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt32(((Number) v).intValue());
        }
        return PrimitiveValue.newInt32(Integer.parseInt(v.toString()));
    }

    private static Value<?> int64FromPojo(Object v) {
        if (v instanceof Long) {
            return PrimitiveValue.newInt64((Long) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt64(((Number) v).longValue());
        }
        return PrimitiveValue.newInt64(Long.parseLong(v.toString()));
    }

    private static Value<?> int8FromPojo(Object v) {
        if (v instanceof Byte) {
            return PrimitiveValue.newInt8((Byte) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newInt8(((Number) v).byteValue());
        }
        return PrimitiveValue.newInt8(Byte.parseByte(v.toString()));
    }

    private static Value<?> intervalFromPojo(Object v) {
        if (v instanceof java.time.Duration) {
            return PrimitiveValue.newInterval((java.time.Duration) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newInterval(java.time.Duration.parse((String) v));
        }
        return PrimitiveValue.newInterval(java.time.Duration.parse(v.toString()));
    }

    private static Value<?> interval64FromPojo(Object v) {
        if (v instanceof java.time.Duration) {
            return PrimitiveValue.newInterval64((java.time.Duration) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newInterval64(java.time.Duration.parse((String) v));
        }
        return PrimitiveValue.newInterval64(java.time.Duration.parse(v.toString()));
    }

    private static Value<?> jsonFromPojo(Object v) {
        return PrimitiveValue.newJson(v.toString());
    }

    private static Value<?> jsonDocumentFromPojo(Object v) {
        return PrimitiveValue.newJsonDocument(v.toString());
    }

    private static Value<?> textFromPojo(Object v) {
        return PrimitiveValue.newText(v.toString());
    }

    private static Value<?> timestampFromPojo(Object v) {
        if (v instanceof java.time.Instant) {
            return PrimitiveValue.newTimestamp((java.time.Instant) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newTimestamp(java.time.Instant.parse((String) v));
        }
        return PrimitiveValue.newTimestamp(java.time.Instant.parse(v.toString()));
    }

    private static Value<?> timestamp64FromPojo(Object v) {
        if (v instanceof java.time.Instant) {
            return PrimitiveValue.newTimestamp64((java.time.Instant) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newTimestamp64(java.time.Instant.parse((String) v));
        }
        return PrimitiveValue.newTimestamp64(java.time.Instant.parse(v.toString()));
    }

    private static Value<?> uint16FromPojo(Object v) {
        if (v instanceof Number) {
            return PrimitiveValue.newUint16(((Number) v).intValue());
        }
        return PrimitiveValue.newUint16(Integer.parseInt(v.toString()));
    }

    private static Value<?> uint32FromPojo(Object v) {
        if (v instanceof Number) {
            return PrimitiveValue.newUint32(((Number) v).longValue());
        }
        return PrimitiveValue.newUint32(Long.parseLong(v.toString()));
    }

    private static Value<?> uint64FromPojo(Object v) {
        if (v instanceof YdbUnsigned) {
            return PrimitiveValue.newUint64(((YdbUnsigned) v).getValue());
        }
        if (v instanceof Long) {
            return PrimitiveValue.newUint64((Long) v);
        }
        if (v instanceof Number) {
            return PrimitiveValue.newUint64(((Number) v).longValue());
        }
        if (v instanceof String) {
            return PrimitiveValue.newUint64(Long.parseUnsignedLong((String) v));
        }
        return PrimitiveValue.newUint64(Long.parseUnsignedLong(v.toString()));
    }

    private static Value<?> uint8FromPojo(Object v) {
        if (v instanceof Number) {
            return PrimitiveValue.newUint8(((Number) v).intValue());
        }
        return PrimitiveValue.newUint8(Integer.parseInt(v.toString()));
    }

    private static Value<?> uuidFromPojo(Object v) {
        if (v instanceof java.util.UUID) {
            return PrimitiveValue.newUuid((java.util.UUID) v);
        }
        if (v instanceof String) {
            return PrimitiveValue.newUuid(java.util.UUID.fromString((String) v));
        }
        return PrimitiveValue.newUuid(java.util.UUID.fromString(v.toString()));
    }

    public static Comparable<?> toPojo(Value<?> v) {
        if (v==null) {
            return null;
        }
        switch (v.getType().getKind()) {
            case OPTIONAL:
                if ( v.asOptional().isPresent() ) {
                    return toPojo(v.asOptional().get());
                } else {
                    return null;
                }
            case DECIMAL:
                return ((DecimalValue)v).toBigDecimal();
            case PRIMITIVE:
                return toPojo(v.asData());
            case NULL:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + v.getType());
        }
    }

    public static Comparable<?> toPojo(PrimitiveValue v) {
        switch (v.getType()) {
            case Bool:
                return v.getBool();
            case Bytes:
                return new YdbBytes(v.getBytes());
            case Date:
                return v.getDate();
            case Date32:
                return v.getDate32();
            case Datetime:
                return v.getDatetime();
            case Datetime64:
                return v.getDatetime64();
            case Double:
                return v.getDouble();
            case Float:
                return v.getFloat();
            case Int16:
                return v.getInt16();
            case Int32:
                return v.getInt32();
            case Int64:
                return v.getInt64();
            case Int8:
                return v.getInt8();
            case Interval:
                return v.getInterval();
            case Interval64:
                return v.getInterval64();
            case Json:
                return v.getJson();
            case JsonDocument:
                return v.getJsonDocument();
            case Text:
                return v.getText();
            case Timestamp:
                return v.getTimestamp();
            case Timestamp64:
                return v.getTimestamp64();
            case Uint16:
                return v.getUint16();
            case Uint32:
                return v.getUint32();
            case Uint64:
                return new YdbUnsigned(v.getUint64());
            case Uint8:
                return v.getUint8();
            case Uuid:
                return v.getUuidString();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + v.getType());
        }
    }

}
