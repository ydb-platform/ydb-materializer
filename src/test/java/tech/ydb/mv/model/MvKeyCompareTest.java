package tech.ydb.mv.model;

import tech.ydb.mv.data.MvKeyPrefix;
import tech.ydb.mv.data.MvKey;
import java.math.BigDecimal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.data.YdbBytes;
import tech.ydb.mv.data.YdbStruct;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;

/**
 * Comprehensive test for MvKey.compareTo() method.
 * Tests comparison with different key lengths (1, 2, 3) and data types
 * (Integer, Long, YdbBytes, String, BigDecimal).
 *
 * @author zinal
 */
public class MvKeyCompareTest {

    // Test data for YdbBytes (only used ones)
    private static final YdbBytes BYTES_SMALL = new YdbBytes(new byte[]{1, 2, 3});
    private static final YdbBytes BYTES_LARGE = new YdbBytes(new byte[]{1, 2, 4});

    private static YdbStruct YS() {
        return new YdbStruct();
    }

    // Helper method to create MvKey from JSON
    private static MvKey createKey(YdbStruct ys, MvKeyInfo keyInfo) {
        return new MvKey(ys, keyInfo);
    }

    // Helper method to create MvKey with YdbBytes
    private MvKey createKeyWithBytes(MvKeyInfo keyInfo, YdbBytes bytesValue) {
        if (keyInfo.size() != 1) {
            throw new IllegalArgumentException();
        }
        String json = new YdbStruct().add(keyInfo.getName(0), bytesValue).toJson();;
        MvKey ret = new MvKey(json, keyInfo);
        Assertions.assertTrue(ret.size()==1);
        Assertions.assertTrue(ret.getValue(0) instanceof YdbBytes);
        return ret;
    }

    // Helper methods to create table info for different key configurations
    private MvTableInfo createSingleIntKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", PrimitiveType.Int32)
                .addKey("key1")
                .build();
    }

    private MvTableInfo createSingleLongKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", PrimitiveType.Int64)
                .addKey("key1")
                .build();
    }

    private MvTableInfo createSingleStringKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", PrimitiveType.Text)
                .addKey("key1")
                .build();
    }

    private MvTableInfo createSingleBytesKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", PrimitiveType.Bytes)
                .addKey("key1")
                .build();
    }

    private MvTableInfo createSingleDecimalKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", DecimalType.of(22, 9))
                .addKey("key1")
                .build();
    }

    private MvTableInfo createDoubleKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", PrimitiveType.Int32)
                .addColumn("key2", PrimitiveType.Text)
                .addKey("key1")
                .addKey("key2")
                .build();
    }

    private MvTableInfo createTripleKeyTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("key1", PrimitiveType.Int32)
                .addColumn("key2", PrimitiveType.Text)
                .addColumn("key3", PrimitiveType.Int64)
                .addKey("key1")
                .addKey("key2")
                .addKey("key3")
                .build();
    }


    @Test
    public void basicComparisonsTest() {
        MvTableInfo tableInfo = MvTableInfo.newBuilder("aaa")
                .addColumn("a", PrimitiveType.Int32)
                .addColumn("b", PrimitiveType.Int32)
                .addKey("a").addKey("b")
                .build();

        MvKeyPrefix p1 = new MvKeyPrefix(new YdbStruct().add("a", 10), tableInfo);
        MvKeyPrefix p2 = new MvKeyPrefix(new YdbStruct().add("a", 20), tableInfo);

        MvKey k1 = new MvKey(new YdbStruct().add("a", 10).add("b", 20), tableInfo);
        MvKey k2 = new MvKey(new YdbStruct().add("a", 15).add("b", 20), tableInfo);
        MvKey k3 = new MvKey(new YdbStruct().add("a", 20).add("b", 20), tableInfo);
        MvKey k4 = new MvKey(new YdbStruct().add("a", 25).add("b", 20), tableInfo);

        int cmp;

        cmp = p1.compareTo(k1);
        Assertions.assertEquals(0, cmp);

        cmp = p1.compareTo(k2);
        Assertions.assertEquals(-1, cmp);

        cmp = p1.compareTo(k3);
        Assertions.assertEquals(-1, cmp);

        cmp = p1.compareTo(k4);
        Assertions.assertEquals(-1, cmp);

        cmp = p2.compareTo(k1);
        Assertions.assertEquals(1, cmp);

        cmp = p2.compareTo(k2);
        Assertions.assertEquals(1, cmp);

        cmp = p2.compareTo(k3);
        Assertions.assertEquals(0, cmp);

        cmp = p2.compareTo(k4);
        Assertions.assertEquals(-1, cmp);
    }

    // Tests for single-element keys
    @Test
    public void testSingleIntegerKeyComparison() {
        MvTableInfo tableInfo = createSingleIntKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvKey key1 = createKey(YS().add("key1", 10), keyInfo);
        MvKey key2 = createKey(YS().add("key1", 20), keyInfo);
        MvKey key3 = createKey(YS().add("key1", 10), keyInfo);

        // Test less than
        Assertions.assertTrue(key1.compareTo(key2) < 0, "10 should be less than 20");

        // Test greater than
        Assertions.assertTrue(key2.compareTo(key1) > 0, "20 should be greater than 10");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key3), "10 should equal 10");
    }

    @Test
    public void testSingleLongKeyComparison() {
        MvTableInfo tableInfo = createSingleLongKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvKey key1 = createKey(YS().add("key1", 100L), keyInfo);
        MvKey key2 = createKey(YS().add("key1", 200L), keyInfo);
        MvKey key3 = createKey(YS().add("key1", 100L), keyInfo);

        // Test less than
        Assertions.assertTrue(key1.compareTo(key2) < 0, "100 should be less than 200");

        // Test greater than
        Assertions.assertTrue(key2.compareTo(key1) > 0, "200 should be greater than 100");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key3), "100 should equal 100");
    }

    @Test
    public void testSingleStringKeyComparison() {
        MvTableInfo tableInfo = createSingleStringKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvKey key1 = createKey(YS().add("key1", "apple"), keyInfo);
        MvKey key2 = createKey(YS().add("key1", "banana"), keyInfo);
        MvKey key3 = createKey(YS().add("key1", "apple"), keyInfo);

        // Test less than
        Assertions.assertTrue(key1.compareTo(key2) < 0, "apple should be less than banana");

        // Test greater than
        Assertions.assertTrue(key2.compareTo(key1) > 0, "banana should be greater than apple");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key3), "apple should equal apple");
    }

    @Test
    public void testSingleBytesKeyComparison() {
        MvTableInfo tableInfo = createSingleBytesKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        // Create MvKey instances using reflection to set YdbBytes values
        // since JSON deserialization won't work for bytes
        MvKey key1 = createKeyWithBytes(keyInfo, BYTES_SMALL);
        MvKey key2 = createKeyWithBytes(keyInfo, BYTES_LARGE);
        MvKey key3 = createKeyWithBytes(keyInfo, BYTES_SMALL);

        // Test less than
        Assertions.assertTrue(key1.compareTo(key2) < 0, "bytes [1,2,3] should be less than [1,2,4]");

        // Test greater than
        Assertions.assertTrue(key2.compareTo(key1) > 0, "bytes [1,2,4] should be greater than [1,2,3]");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key3), "bytes [1,2,3] should equal [1,2,3]");
    }

    @Test
    public void testSingleDecimalKeyComparison() {
        MvTableInfo tableInfo = createSingleDecimalKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvKey key1 = createKey(YS().add("key1", new BigDecimal("10.5")), keyInfo);
        MvKey key2 = createKey(YS().add("key1", new BigDecimal("20.5")), keyInfo);
        MvKey key3 = createKey(YS().add("key1", new BigDecimal("10.5")), keyInfo);

        // Test less than
        Assertions.assertTrue(key1.compareTo(key2) < 0, "10.5 should be less than 20.5");

        // Test greater than
        Assertions.assertTrue(key2.compareTo(key1) > 0, "20.5 should be greater than 10.5");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key3), "10.5 should equal 10.5");
    }

    // Tests for two-element keys
    @Test
    public void testDoubleKeyComparison() {
        MvTableInfo tableInfo = createDoubleKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvKey key1 = createKey(YS().add("key1", 10).add("key2", "apple"), keyInfo);
        MvKey key2 = createKey(YS().add("key1", 10).add("key2", "banana"), keyInfo);
        MvKey key3 = createKey(YS().add("key1", 20).add("key2", "apple"), keyInfo);
        MvKey key4 = createKey(YS().add("key1", 10).add("key2", "apple"), keyInfo);

        // Test lexicographic comparison - first key different
        Assertions.assertTrue(key1.compareTo(key3) < 0, "[10, apple] should be less than [20, apple]");
        Assertions.assertTrue(key3.compareTo(key1) > 0, "[20, apple] should be greater than [10, apple]");

        // Test lexicographic comparison - second key different
        Assertions.assertTrue(key1.compareTo(key2) < 0, "[10, apple] should be less than [10, banana]");
        Assertions.assertTrue(key2.compareTo(key1) > 0, "[10, banana] should be greater than [10, apple]");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key4), "[10, apple] should equal [10, apple]");
    }

    // Tests for three-element keys
    @Test
    public void testTripleKeyComparison() {
        MvTableInfo tableInfo = createTripleKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvKey key1 = createKey(YS().add("key1", 10).add("key2", "apple").add("key3", 100L), keyInfo);
        MvKey key2 = createKey(YS().add("key1", 10).add("key2", "apple").add("key3", 200L), keyInfo);
        MvKey key3 = createKey(YS().add("key1", 10).add("key2", "banana").add("key3", 100L), keyInfo);
        MvKey key4 = createKey(YS().add("key1", 20).add("key2", "apple").add("key3", 100L), keyInfo);
        MvKey key5 = createKey(YS().add("key1", 10).add("key2", "apple").add("key3", 100L), keyInfo);

        // Test lexicographic comparison - first key different
        Assertions.assertTrue(key1.compareTo(key4) < 0, "[10, apple, 100] should be less than [20, apple, 100]");

        // Test lexicographic comparison - second key different
        Assertions.assertTrue(key1.compareTo(key3) < 0, "[10, apple, 100] should be less than [10, banana, 100]");

        // Test lexicographic comparison - third key different
        Assertions.assertTrue(key1.compareTo(key2) < 0, "[10, apple, 100] should be less than [10, apple, 200]");

        // Test equals
        Assertions.assertEquals(0, key1.compareTo(key5), "[10, apple, 100] should equal [10, apple, 100]");
    }

    // Tests for null handling
    @Test
    public void testNullHandling() {
        MvTableInfo tableInfo = createDoubleKeyTable("test_table");
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        // Create keys with null values
        MvKey keyWithNulls = createKey(YS(), keyInfo);
        MvKey keyWithFirstNull = createKey(YS().add("key2", "apple"), keyInfo);
        MvKey keyWithSecondNull = createKey(YS().add("key1", 10), keyInfo);
        MvKey keyWithoutNulls = createKey(YS().add("key1", 10).add("key2", "apple"), keyInfo);

        // Test null < non-null for first element
        Assertions.assertTrue(keyWithFirstNull.compareTo(keyWithoutNulls) < 0,
                "[null, apple] should be less than [10, apple]");
        Assertions.assertTrue(keyWithoutNulls.compareTo(keyWithFirstNull) > 0,
                "[10, apple] should be greater than [null, apple]");

        // Test null < non-null for second element (when first elements are equal)
        Assertions.assertTrue(keyWithSecondNull.compareTo(keyWithoutNulls) < 0,
                "[10, null] should be less than [10, apple]");
        Assertions.assertTrue(keyWithoutNulls.compareTo(keyWithSecondNull) > 0,
                "[10, apple] should be greater than [10, null]");

        // Test null == null
        Assertions.assertEquals(0, keyWithNulls.compareTo(keyWithNulls),
                "[null, null] should equal [null, null]");

        // Test when first element is null, second element comparison doesn't matter
        MvKey anotherKeyWithFirstNull = createKey(YS().add("key2", "banana"), keyInfo);
        Assertions.assertTrue(keyWithFirstNull.compareTo(keyWithoutNulls) < 0,
                "[null, apple] should be less than [10, apple]");
        Assertions.assertTrue(anotherKeyWithFirstNull.compareTo(keyWithoutNulls) < 0,
                "[null, banana] should be less than [10, apple]");
    }

    // Tests for error cases
    @Test
    public void testDifferentKeyInfoComparison() {
        MvTableInfo tableInfo1 = createSingleIntKeyTable("table1");
        MvTableInfo tableInfo2 = createSingleStringKeyTable("table2");

        MvKey key1 = createKey(YS().add("key1", 10), tableInfo1.getKeyInfo());
        MvKey key2 = createKey(YS().add("key1", "apple"), tableInfo2.getKeyInfo());

        // Should throw IllegalArgumentException when comparing keys with different key info
        Assertions.assertThrows(IllegalArgumentException.class, () -> key1.compareTo(key2),
                "Should throw exception when comparing keys with different key info");
    }

    @Test
    public void testDifferentKeyLengthComparison() {
        MvTableInfo singleKeyTable = createSingleIntKeyTable("table1");
        MvTableInfo doubleKeyTable = createDoubleKeyTable("table2");

        MvKey key1 = createKey(YS().add("key1", 10), singleKeyTable.getKeyInfo());
        MvKey key2 = createKey(YS().add("key1", 10).add("key2", "apple"), doubleKeyTable.getKeyInfo());

        // Should throw IllegalArgumentException when comparing keys with different lengths
        Assertions.assertThrows(IllegalArgumentException.class, () -> key1.compareTo(key2),
                "Should throw exception when comparing keys with different lengths");
    }

}
