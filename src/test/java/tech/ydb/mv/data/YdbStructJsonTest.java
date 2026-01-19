package tech.ydb.mv.data;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test for YdbStruct toJson() and fromJson() methods.
 * Tests that toJson() works on all supported data types (from CLS2INFO member)
 * and that fromJson() applied to toJson() output always returns an instance
 * which is equal to the original input.
 *
 * @author zinal
 */
public class YdbStructJsonTest {

    @Test
    public void testBooleanJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("boolTrue", Boolean.TRUE);
        original.add("boolFalse", Boolean.FALSE);

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Boolean values should survive JSON round trip");
        Assertions.assertEquals(Boolean.TRUE, restored.get("boolTrue"));
        Assertions.assertEquals(Boolean.FALSE, restored.get("boolFalse"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testStringJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("simpleString", "hello world");
        original.add("emptyString", "");
        original.add("specialChars", "Hello, 世界! 🌍 \"quotes\" and 'apostrophes'");
        original.add("unicodeString", "Привет мир! こんにちは世界！");

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "String values should survive JSON round trip");
        Assertions.assertEquals("hello world", restored.get("simpleString"));
        Assertions.assertEquals("", restored.get("emptyString"));
        Assertions.assertEquals("Hello, 世界! 🌍 \"quotes\" and 'apostrophes'", restored.get("specialChars"));
        Assertions.assertEquals("Привет мир! こんにちは世界！", restored.get("unicodeString"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testYdbBytesJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("bytes1", new YdbBytes(new byte[]{1, 2, 3, 4, 5}));
        original.add("bytes2", new YdbBytes(new byte[]{-128, -1, 0, 1, 127}));
        original.add("emptyBytes", new YdbBytes(new byte[]{}));
        original.add("textBytes", new YdbBytes("Hello World!".getBytes()));

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "YdbBytes values should survive JSON round trip");

        YdbBytes restoredBytes1 = (YdbBytes) restored.get("bytes1");
        YdbBytes restoredBytes2 = (YdbBytes) restored.get("bytes2");
        YdbBytes restoredEmptyBytes = (YdbBytes) restored.get("emptyBytes");
        YdbBytes restoredTextBytes = (YdbBytes) restored.get("textBytes");

        Assertions.assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, restoredBytes1.getValue());
        Assertions.assertArrayEquals(new byte[]{-128, -1, 0, 1, 127}, restoredBytes2.getValue());
        Assertions.assertArrayEquals(new byte[]{}, restoredEmptyBytes.getValue());
        Assertions.assertArrayEquals("Hello World!".getBytes(), restoredTextBytes.getValue());

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testYdbUnsignedJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("unsignedZero", new YdbUnsigned(0L));
        original.add("unsignedSmall", new YdbUnsigned(42L));
        original.add("unsignedLarge", new YdbUnsigned(Long.MAX_VALUE));
        original.add("unsignedMaxValue", new YdbUnsigned(-1L)); // Maximum unsigned value

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "YdbUnsigned values should survive JSON round trip");
        Assertions.assertEquals(0L, ((YdbUnsigned) restored.get("unsignedZero")).getValue());
        Assertions.assertEquals(42L, ((YdbUnsigned) restored.get("unsignedSmall")).getValue());
        Assertions.assertEquals(Long.MAX_VALUE, ((YdbUnsigned) restored.get("unsignedLarge")).getValue());
        Assertions.assertEquals(-1L, ((YdbUnsigned) restored.get("unsignedMaxValue")).getValue());

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testFloatJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("floatZero", 0.0f);
        original.add("floatPositive", 3.14159f);
        original.add("floatNegative", -2.71828f);
        original.add("floatMax", Float.MAX_VALUE);
        original.add("floatMin", Float.MIN_VALUE);
        original.add("floatMinNormal", Float.MIN_NORMAL);

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Float values should survive JSON round trip");
        Assertions.assertEquals(0.0f, (Float) restored.get("floatZero"), 0.0f);
        Assertions.assertEquals(3.14159f, (Float) restored.get("floatPositive"), 0.0001f);
        Assertions.assertEquals(-2.71828f, (Float) restored.get("floatNegative"), 0.0001f);
        Assertions.assertEquals(Float.MAX_VALUE, (Float) restored.get("floatMax"), 0.0f);
        Assertions.assertEquals(Float.MIN_VALUE, (Float) restored.get("floatMin"), 0.0f);
        Assertions.assertEquals(Float.MIN_NORMAL, (Float) restored.get("floatMinNormal"), 0.0f);

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testDoubleJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("doubleZero", 0.0);
        original.add("doublePositive", 3.141592653589793);
        original.add("doubleNegative", -2.718281828459045);
        original.add("doubleMax", Double.MAX_VALUE);
        original.add("doubleMin", Double.MIN_VALUE);
        original.add("doubleMinNormal", Double.MIN_NORMAL);

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Double values should survive JSON round trip");
        Assertions.assertEquals(0.0, (Double) restored.get("doubleZero"), 0.0);
        Assertions.assertEquals(3.141592653589793, (Double) restored.get("doublePositive"), 0.000000000000001);
        Assertions.assertEquals(-2.718281828459045, (Double) restored.get("doubleNegative"), 0.000000000000001);
        Assertions.assertEquals(Double.MAX_VALUE, (Double) restored.get("doubleMax"), 0.0);
        Assertions.assertEquals(Double.MIN_VALUE, (Double) restored.get("doubleMin"), 0.0);
        Assertions.assertEquals(Double.MIN_NORMAL, (Double) restored.get("doubleMinNormal"), 0.0);

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testShortJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("shortZero", (short) 0);
        original.add("shortPositive", (short) 12345);
        original.add("shortNegative", (short) -6789);
        original.add("shortMax", Short.MAX_VALUE);
        original.add("shortMin", Short.MIN_VALUE);

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Short values should survive JSON round trip");
        Assertions.assertEquals((short) 0, (Short) restored.get("shortZero"));
        Assertions.assertEquals((short) 12345, (Short) restored.get("shortPositive"));
        Assertions.assertEquals((short) -6789, (Short) restored.get("shortNegative"));
        Assertions.assertEquals(Short.MAX_VALUE, (Short) restored.get("shortMax"));
        Assertions.assertEquals(Short.MIN_VALUE, (Short) restored.get("shortMin"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testIntegerJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("intZero", 0);
        original.add("intPositive", 123456789);
        original.add("intNegative", -987654321);
        original.add("intMax", Integer.MAX_VALUE);
        original.add("intMin", Integer.MIN_VALUE);

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Integer values should survive JSON round trip");
        Assertions.assertEquals(0, (Integer) restored.get("intZero"));
        Assertions.assertEquals(123456789, (Integer) restored.get("intPositive"));
        Assertions.assertEquals(-987654321, (Integer) restored.get("intNegative"));
        Assertions.assertEquals(Integer.MAX_VALUE, (Integer) restored.get("intMax"));
        Assertions.assertEquals(Integer.MIN_VALUE, (Integer) restored.get("intMin"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testLongJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("longZero", 0L);
        original.add("longPositive", 1234567890123456789L);
        original.add("longNegative", -987654321098765432L);
        original.add("longMax", Long.MAX_VALUE);
        original.add("longMin", Long.MIN_VALUE);

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Long values should survive JSON round trip");
        Assertions.assertEquals(0L, (Long) restored.get("longZero"));
        Assertions.assertEquals(1234567890123456789L, (Long) restored.get("longPositive"));
        Assertions.assertEquals(-987654321098765432L, (Long) restored.get("longNegative"));
        Assertions.assertEquals(Long.MAX_VALUE, (Long) restored.get("longMax"));
        Assertions.assertEquals(Long.MIN_VALUE, (Long) restored.get("longMin"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testBigDecimalJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("decimalZero", BigDecimal.ZERO);
        original.add("decimalOne", BigDecimal.ONE);
        original.add("decimalTen", BigDecimal.TEN);
        original.add("decimalPi", new BigDecimal("3.141592653589793238462643383279"));
        original.add("decimalLarge", new BigDecimal("123456789012345678901234567890.123456789"));
        original.add("decimalNegative", new BigDecimal("-987654321098765432109876543210.987654321"));

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "BigDecimal values should survive JSON round trip");
        Assertions.assertEquals(BigDecimal.ZERO, (BigDecimal) restored.get("decimalZero"));
        Assertions.assertEquals(BigDecimal.ONE, (BigDecimal) restored.get("decimalOne"));
        Assertions.assertEquals(BigDecimal.TEN, (BigDecimal) restored.get("decimalTen"));
        Assertions.assertEquals(new BigDecimal("3.141592653589793238462643383279"),
                                (BigDecimal) restored.get("decimalPi"));
        Assertions.assertEquals(new BigDecimal("123456789012345678901234567890.123456789"),
                                (BigDecimal) restored.get("decimalLarge"));
        Assertions.assertEquals(new BigDecimal("-987654321098765432109876543210.987654321"),
                                (BigDecimal) restored.get("decimalNegative"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testLocalDateJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("dateEpoch", LocalDate.of(1970, 1, 1));
        original.add("dateToday", LocalDate.of(2023, 8, 17));
        original.add("dateFuture", LocalDate.of(2100, 12, 31));
        original.add("datePast", LocalDate.of(1900, 1, 1));
        original.add("dateLeapYear", LocalDate.of(2000, 2, 29));

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "LocalDate values should survive JSON round trip");
        Assertions.assertEquals(LocalDate.of(1970, 1, 1), (LocalDate) restored.get("dateEpoch"));
        Assertions.assertEquals(LocalDate.of(2023, 8, 17), (LocalDate) restored.get("dateToday"));
        Assertions.assertEquals(LocalDate.of(2100, 12, 31), (LocalDate) restored.get("dateFuture"));
        Assertions.assertEquals(LocalDate.of(1900, 1, 1), (LocalDate) restored.get("datePast"));
        Assertions.assertEquals(LocalDate.of(2000, 2, 29), (LocalDate) restored.get("dateLeapYear"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testLocalDateTimeJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("dateTimeEpoch", LocalDateTime.of(1970, 1, 1, 0, 0, 0));
        original.add("dateTimeNow", LocalDateTime.of(2023, 8, 17, 15, 45, 30));
        original.add("dateTimePrecise", LocalDateTime.of(2023, 12, 25, 15, 30, 45, 123456789));
        original.add("dateTimeMinNano", LocalDateTime.of(2023, 1, 1, 12, 0, 0, 1));
        original.add("dateTimeMaxNano", LocalDateTime.of(2023, 1, 1, 12, 0, 0, 999999999));

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "LocalDateTime values should survive JSON round trip");
        Assertions.assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                                (LocalDateTime) restored.get("dateTimeEpoch"));
        Assertions.assertEquals(LocalDateTime.of(2023, 8, 17, 15, 45, 30), (LocalDateTime) restored.get("dateTimeNow"));
        Assertions.assertEquals(LocalDateTime.of(2023, 12, 25, 15, 30, 45, 123456789),
                                (LocalDateTime) restored.get("dateTimePrecise"));
        Assertions.assertEquals(LocalDateTime.of(2023, 1, 1, 12, 0, 0, 1),
                                (LocalDateTime) restored.get("dateTimeMinNano"));
        Assertions.assertEquals(LocalDateTime.of(2023, 1, 1, 12, 0, 0, 999999999),
                                (LocalDateTime) restored.get("dateTimeMaxNano"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testInstantJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("instantEpoch", Instant.EPOCH);
        original.add("instantNow", Instant.parse("2023-08-17T15:45:30.123Z"));
        original.add("instantPrecise", Instant.parse("2023-12-25T15:30:45.123456789Z"));
        original.add("instantFuture", Instant.parse("2100-01-01T00:00:00Z"));
        original.add("instantPast", Instant.parse("1900-01-01T00:00:00Z"));

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Instant values should survive JSON round trip");
        Assertions.assertEquals(Instant.EPOCH, (Instant) restored.get("instantEpoch"));
        Assertions.assertEquals(Instant.parse("2023-08-17T15:45:30.123Z"), (Instant) restored.get("instantNow"));
        Assertions.assertEquals(Instant.parse("2023-12-25T15:30:45.123456789Z"),
                                (Instant) restored.get("instantPrecise"));
        Assertions.assertEquals(Instant.parse("2100-01-01T00:00:00Z"),
                                (Instant) restored.get("instantFuture"));
        Assertions.assertEquals(Instant.parse("1900-01-01T00:00:00Z"),
                                (Instant) restored.get("instantPast"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testDurationJsonRoundTrip() {
        YdbStruct original = new YdbStruct();
        original.add("durationZero", Duration.ZERO);
        original.add("durationSeconds", Duration.ofSeconds(3600)); // 1 hour
        original.add("durationMinutes", Duration.ofMinutes(90)); // 1.5 hours
        original.add("durationHours", Duration.ofHours(24)); // 1 day
        original.add("durationDays", Duration.ofDays(7)); // 1 week
        original.add("durationNegative", Duration.ofSeconds(-1800)); // -30 minutes

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Duration values should survive JSON round trip");
        Assertions.assertEquals(Duration.ZERO, (Duration) restored.get("durationZero"));
        Assertions.assertEquals(Duration.ofSeconds(3600), (Duration) restored.get("durationSeconds"));
        Assertions.assertEquals(Duration.ofMinutes(90), (Duration) restored.get("durationMinutes"));
        Assertions.assertEquals(Duration.ofHours(24), (Duration) restored.get("durationHours"));
        Assertions.assertEquals(Duration.ofDays(7), (Duration) restored.get("durationDays"));
        Assertions.assertEquals(Duration.ofSeconds(-1800), (Duration) restored.get("durationNegative"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testMixedTypesJsonRoundTrip() {
        YdbStruct original = new YdbStruct();

        // Add one value of each supported type
        original.add("boolean", Boolean.TRUE);
        original.add("string", "test string");
        original.add("bytes", new YdbBytes("test".getBytes()));
        original.add("unsigned", new YdbUnsigned(42L));
        original.add("float", 3.14f);
        original.add("double", 2.718281828459045);
        original.add("short", (short) 123);
        original.add("integer", 456789);
        original.add("long", 123456789012345L);
        original.add("decimal", new BigDecimal("99.99"));
        original.add("date", LocalDate.of(2023, 6, 15));
        original.add("dateTime", LocalDateTime.of(2023, 6, 15, 14, 30, 0));
        original.add("instant", Instant.parse("2023-06-15T14:30:00Z"));
        original.add("duration", Duration.ofHours(2));

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Mixed types should survive JSON round trip");

        // Verify each type individually
        Assertions.assertEquals(Boolean.TRUE, restored.get("boolean"));
        Assertions.assertEquals("test string", restored.get("string"));
        Assertions.assertArrayEquals("test".getBytes(), ((YdbBytes) restored.get("bytes")).getValue());
        Assertions.assertEquals(42L, ((YdbUnsigned) restored.get("unsigned")).getValue());
        Assertions.assertEquals(3.14f, (Float) restored.get("float"), 0.001f);
        Assertions.assertEquals(2.718281828459045, (Double) restored.get("double"), 0.000000000000001);
        Assertions.assertEquals((short) 123, (Short) restored.get("short"));
        Assertions.assertEquals(456789, (Integer) restored.get("integer"));
        Assertions.assertEquals(123456789012345L, (Long) restored.get("long"));
        Assertions.assertEquals(new BigDecimal("99.99"), (BigDecimal) restored.get("decimal"));
        Assertions.assertEquals(LocalDate.of(2023, 6, 15), (LocalDate) restored.get("date"));
        Assertions.assertEquals(LocalDateTime.of(2023, 6, 15, 14, 30, 0), (LocalDateTime) restored.get("dateTime"));
        Assertions.assertEquals(Instant.parse("2023-06-15T14:30:00Z"), (Instant) restored.get("instant"));
        Assertions.assertEquals(Duration.ofHours(2), (Duration) restored.get("duration"));

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testEmptyStructJsonRoundTrip() {
        YdbStruct original = new YdbStruct();

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        Assertions.assertEquals(original, restored, "Empty struct should survive JSON round trip");
        Assertions.assertTrue(restored.keySet().isEmpty(), "Restored struct should be empty");

        Assertions.assertEquals(original, restored);
    }

    @Test
    public void testNullValuesAreSkipped() {
        YdbStruct original = new YdbStruct();
        original.put("nullValue", (String) null);
        original.add("realValue", "not null");

        String json = original.toJson();
        YdbStruct restored = YdbStruct.fromJson(json);

        // Null values should be skipped in JSON, so the restored struct should only have the non-null value
        Assertions.assertEquals(1, restored.keySet().size(), "Only non-null values should be in restored struct");
        Assertions.assertEquals("not null", restored.get("realValue"));
        Assertions.assertNull(restored.get("nullValue"), "Null values should not be restored");
    }

    @Test
    public void testJsonFormatStructure() {
        YdbStruct original = new YdbStruct();
        original.add("testString", "hello");
        original.add("testInt", 42);

        String json = original.toJson();

        // Verify JSON contains type codes and values
        Assertions.assertTrue(json.contains("\"t\":"), "JSON should contain type codes");
        Assertions.assertTrue(json.contains("\"v\":"), "JSON should contain values");
        Assertions.assertTrue(json.contains("testString"), "JSON should contain field names");
        Assertions.assertTrue(json.contains("testInt"), "JSON should contain field names");
    }
}
