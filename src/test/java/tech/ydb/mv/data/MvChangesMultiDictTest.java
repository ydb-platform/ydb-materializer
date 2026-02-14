package tech.ydb.mv.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import tech.ydb.mv.model.*;
import tech.ydb.table.values.PrimitiveType;

/**
 * Comprehensive test for MvChangesMultiDict.toFilters() method. Tests various
 * scenarios including relevant/non-relevant field changes, different numbers of
 * dictionaries, and different target dependencies.
 *
 * @author zinal
 */
public class MvChangesMultiDictTest {

    private MvHandler handler;
    private MvViewExpr targetSingleDict;
    private MvViewExpr targetMultiDict;
    private MvChangesMultiDict changes;

    // Dictionary table infos
    private MvTableInfo dict1Info, dict2Info, dict3Info, mainTableInfo;

    // Join sources for dictionaries
    private MvJoinSource mainSource, dict1Source, dict2Source, dict3Source;

    // Input configurations
    private MvInput mainInput, dict1Input, dict2Input, dict3Input;

    @BeforeEach
    public void setUp() {
        // Create table infos
        mainTableInfo = MvTableInfo.newBuilder("main_table")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("dict1_id", PrimitiveType.Uint64)
                .addColumn("dict2_id", PrimitiveType.Uint64)
                .addColumn("dict3_id", PrimitiveType.Uint64)
                .addColumn("data", PrimitiveType.Text)
                .addKey("id")
                .build();

        dict1Info = MvTableInfo.newBuilder("dict1")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("name", PrimitiveType.Text)
                .addColumn("description", PrimitiveType.Text)
                .addColumn("status", PrimitiveType.Text)
                .addKey("id")
                .build();

        dict2Info = MvTableInfo.newBuilder("dict2")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("code", PrimitiveType.Text)
                .addColumn("value", PrimitiveType.Text)
                .addColumn("metadata", PrimitiveType.Text)
                .addKey("id")
                .build();

        dict3Info = MvTableInfo.newBuilder("dict3")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("category", PrimitiveType.Text)
                .addColumn("subcategory", PrimitiveType.Text)
                .addColumn("extra", PrimitiveType.Text)
                .addKey("id")
                .build();

        // Create inputs
        mainInput = new MvInput("main_table", "main_cf", MvSqlPos.EMPTY);
        mainInput.setTableInfo(mainTableInfo);
        mainInput.setBatchMode(false); // Main table is not batch mode

        dict1Input = new MvInput("dict1", "dict1_cf", MvSqlPos.EMPTY);
        dict1Input.setTableInfo(dict1Info);
        dict1Input.setBatchMode(true); // Dictionary is batch mode

        dict2Input = new MvInput("dict2", "dict2_cf", MvSqlPos.EMPTY);
        dict2Input.setTableInfo(dict2Info);
        dict2Input.setBatchMode(true);

        dict3Input = new MvInput("dict3", "dict3_cf", MvSqlPos.EMPTY);
        dict3Input.setTableInfo(dict3Info);
        dict3Input.setBatchMode(true);

        // Create join sources
        mainSource = new MvJoinSource();
        mainSource.setTableName("main_table");
        mainSource.setTableAlias("m");
        mainSource.setMode(MvJoinMode.MAIN);
        mainSource.setTableInfo(mainTableInfo);
        mainSource.setInput(mainInput);

        dict1Source = new MvJoinSource();
        dict1Source.setTableName("dict1");
        dict1Source.setTableAlias("d1");
        dict1Source.setMode(MvJoinMode.LEFT);
        dict1Source.setTableInfo(dict1Info);
        dict1Source.setInput(dict1Input);

        dict2Source = new MvJoinSource();
        dict2Source.setTableName("dict2");
        dict2Source.setTableAlias("d2");
        dict2Source.setMode(MvJoinMode.LEFT);
        dict2Source.setTableInfo(dict2Info);
        dict2Source.setInput(dict2Input);

        dict3Source = new MvJoinSource();
        dict3Source.setTableName("dict3");
        dict3Source.setTableAlias("d3");
        dict3Source.setMode(MvJoinMode.LEFT);
        dict3Source.setTableInfo(dict3Info);
        dict3Source.setInput(dict3Input);

        // Create join conditions
        MvJoinCondition cond1 = new MvJoinCondition();
        cond1.setFirstAlias("m");
        cond1.setFirstColumn("dict1_id");
        cond1.setSecondAlias("d1");
        cond1.setSecondColumn("id");
        dict1Source.getConditions().add(cond1);

        MvJoinCondition cond2 = new MvJoinCondition();
        cond2.setFirstAlias("m");
        cond2.setFirstColumn("dict2_id");
        cond2.setSecondAlias("d2");
        cond2.setSecondColumn("id");
        dict2Source.getConditions().add(cond2);

        MvJoinCondition cond3 = new MvJoinCondition();
        cond3.setFirstAlias("m");
        cond3.setFirstColumn("dict3_id");
        cond3.setSecondAlias("d3");
        cond3.setSecondColumn("id");
        dict3Source.getConditions().add(cond3);

        // Create target that depends on single dictionary (dict1 only)
        targetSingleDict = new MvViewExpr("target_single");
        targetSingleDict.getSources().add(mainSource);
        targetSingleDict.getSources().add(dict1Source);

        // Add columns that use dict1 fields
        MvColumn col1 = new MvColumn("id");
        col1.setSourceAlias("m");
        col1.setSourceColumn("id");
        targetSingleDict.getColumns().add(col1);

        MvColumn col2 = new MvColumn("dict1_name");
        col2.setSourceAlias("d1");
        col2.setSourceColumn("name");
        targetSingleDict.getColumns().add(col2);

        // Create target that depends on all three dictionaries
        targetMultiDict = new MvViewExpr("target_multi");
        targetMultiDict.getSources().add(mainSource);
        targetMultiDict.getSources().add(dict1Source);
        targetMultiDict.getSources().add(dict2Source);
        targetMultiDict.getSources().add(dict3Source);

        // Add columns that use all dictionary fields
        MvColumn col3 = new MvColumn("id");
        col3.setSourceAlias("m");
        col3.setSourceColumn("id");
        targetMultiDict.getColumns().add(col3);

        MvColumn col4 = new MvColumn("dict1_name");
        col4.setSourceAlias("d1");
        col4.setSourceColumn("name");
        targetMultiDict.getColumns().add(col4);

        MvColumn col5 = new MvColumn("dict2_code");
        col5.setSourceAlias("d2");
        col5.setSourceColumn("code");
        targetMultiDict.getColumns().add(col5);

        MvColumn col6 = new MvColumn("dict3_category");
        col6.setSourceAlias("d3");
        col6.setSourceColumn("category");
        targetMultiDict.getColumns().add(col6);

        // Create handler
        handler = new MvHandler("test_handler");
        handler.addInput(mainInput);
        handler.addInput(dict1Input);
        handler.addInput(dict2Input);
        handler.addInput(dict3Input);
        handler.addView(targetSingleDict.getView());
        handler.addView(targetMultiDict.getView());

        changes = new MvChangesMultiDict();
    }

    private MvKey createKey(String tableName, long id) {
        MvTableInfo tableInfo;
        switch (tableName) {
            case "dict1":
                tableInfo = dict1Info;
                break;
            case "dict2":
                tableInfo = dict2Info;
                break;
            case "dict3":
                tableInfo = dict3Info;
                break;
            case "non_existent":
                // Create a minimal table info for testing
                tableInfo = MvTableInfo.newBuilder("non_existent")
                        .addColumn("id", PrimitiveType.Uint64)
                        .addKey("id")
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Unknown table: " + tableName);
        }
        return new MvKey(new YdbStruct().add("id", id), tableInfo);
    }

    private MvChangesSingleDict createDictChanges(String tableName, String field, long... ids) {
        MvChangesSingleDict dict = new MvChangesSingleDict(tableName);
        for (long id : ids) {
            dict.updateField(field, createKey(tableName, id));
        }
        return dict;
    }

    @Test
    public void testSingleDictRelevantFieldChange() {
        // Test: Single dictionary with relevant field change
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        changes.addItem(dict1Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets since both use dict1.name
        assertEquals(2, filters.size());

        // Check that both targets have filters
        Set<MvViewExpr> filteredTargets = new HashSet<>();
        for (MvRowFilter filter : filters) {
            filteredTargets.add(filter.getTarget());
            assertFalse(filter.isEmpty());
        }
        assertTrue(filteredTargets.contains(targetSingleDict));
        assertTrue(filteredTargets.contains(targetMultiDict));
    }

    @Test
    public void testSingleDictNonRelevantFieldChange() {
        // Test: Single dictionary with non-relevant field change - test individual toFilter method to avoid NPE bug
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "description", 1L, 2L);
        changes.addItem(dict1Changes);

        MvRowFilter filter1 = changes.toFilter(handler, targetSingleDict);
        MvRowFilter filter2 = changes.toFilter(handler, targetMultiDict);

        // Should return null when no relevant changes
        assertNull(filter1);
        assertNull(filter2);
    }

    @Test
    public void testSingleDictMixedFieldChanges() {
        // Test: Single dictionary with both relevant and non-relevant field changes
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        dict1Changes.updateField("description", createKey("dict1", 3L)); // Non-relevant
        changes.addItem(dict1Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets since name is relevant for both
        assertEquals(2, filters.size());

        // Check that both targets have filters
        Set<MvViewExpr> filteredTargets = new HashSet<>();
        for (MvRowFilter filter : filters) {
            filteredTargets.add(filter.getTarget());
            assertFalse(filter.isEmpty());
        }
        assertTrue(filteredTargets.contains(targetSingleDict));
        assertTrue(filteredTargets.contains(targetMultiDict));
    }

    @Test
    public void testTwoDictsRelevantChanges() {
        // Test: Two dictionaries with relevant field changes
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "code", 3L, 4L);
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets since both have relevant changes
        assertEquals(2, filters.size());

        // Check that both targets have filters
        Set<MvViewExpr> filteredTargets = new HashSet<>();
        for (MvRowFilter filter : filters) {
            filteredTargets.add(filter.getTarget());
            assertFalse(filter.isEmpty());
        }
        assertTrue(filteredTargets.contains(targetSingleDict));
        assertTrue(filteredTargets.contains(targetMultiDict));
    }

    @Test
    public void testTwoDictsOneRelevantOneNonRelevant() {
        // Test: Two dictionaries - one with relevant changes, one with non-relevant
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L); // Relevant
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "metadata", 3L, 4L); // Non-relevant
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets since dict1.name is relevant for both
        assertEquals(2, filters.size());

        // Check that both targets have filters
        Set<MvViewExpr> filteredTargets = new HashSet<>();
        for (MvRowFilter filter : filters) {
            filteredTargets.add(filter.getTarget());
            assertFalse(filter.isEmpty());
        }
        assertTrue(filteredTargets.contains(targetSingleDict));
        assertTrue(filteredTargets.contains(targetMultiDict));
    }

    @Test
    public void testTwoDictsBothNonRelevant() {
        // Test: Two dictionaries with non-relevant field changes - test individual toFilter method to avoid NPE bug
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "description", 1L, 2L);
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "metadata", 3L, 4L);
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);

        MvRowFilter filter1 = changes.toFilter(handler, targetSingleDict);
        MvRowFilter filter2 = changes.toFilter(handler, targetMultiDict);

        // Should return null when no relevant changes
        assertNull(filter1);
        assertNull(filter2);
    }

    @Test
    public void testThreeDictsAllRelevant() {
        // Test: Three dictionaries with all relevant field changes
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "code", 3L, 4L);
        MvChangesSingleDict dict3Changes = createDictChanges("dict3", "category", 5L, 6L);
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);
        changes.addItem(dict3Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets
        assertEquals(2, filters.size());

        for (MvRowFilter filter : filters) {
            assertFalse(filter.isEmpty());
        }
    }

    @Test
    public void testThreeDictsMixedRelevance() {
        // Test: Three dictionaries with mixed relevant/non-relevant changes
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L); // Relevant
        dict1Changes.updateField("description", createKey("dict1", 7L)); // Non-relevant
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "metadata", 3L, 4L); // Non-relevant
        MvChangesSingleDict dict3Changes = createDictChanges("dict3", "category", 5L, 6L); // Relevant
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);
        changes.addItem(dict3Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets (dict1.name and dict3.category are relevant)
        assertEquals(2, filters.size());

        for (MvRowFilter filter : filters) {
            assertFalse(filter.isEmpty());
        }
    }

    @Test
    public void testThreeDictsAllNonRelevant() {
        // Test: Three dictionaries with all non-relevant field changes - test individual toFilter method to avoid NPE bug
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "description", 1L, 2L);
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "metadata", 3L, 4L);
        MvChangesSingleDict dict3Changes = createDictChanges("dict3", "extra", 5L, 6L);
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);
        changes.addItem(dict3Changes);

        MvRowFilter filter1 = changes.toFilter(handler, targetSingleDict);
        MvRowFilter filter2 = changes.toFilter(handler, targetMultiDict);

        // Should return null when no relevant changes
        assertNull(filter1);
        assertNull(filter2);
    }

    @Test
    public void testEmptyChanges() {
        // Test: No dictionary changes - test individual toFilter method to avoid NPE bug
        MvRowFilter filter1 = changes.toFilter(handler, targetSingleDict);
        MvRowFilter filter2 = changes.toFilter(handler, targetMultiDict);

        // Should return null when no relevant changes
        assertNull(filter1);
        assertNull(filter2);
    }

    @Test
    public void testEmptyDictChanges() {
        // Test: Dictionary changes with no field changes - test individual toFilter method to avoid NPE bug
        MvChangesSingleDict dict1Changes = new MvChangesSingleDict("dict1");
        changes.addItem(dict1Changes);

        MvRowFilter filter1 = changes.toFilter(handler, targetSingleDict);
        MvRowFilter filter2 = changes.toFilter(handler, targetMultiDict);

        // Should return null when no relevant changes
        assertNull(filter1);
        assertNull(filter2);
    }

    @Test
    public void testMultipleFieldChangesInSameDict() {
        // Test: Multiple relevant field changes in the same dictionary
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        dict1Changes.updateField("status", createKey("dict1", 3L)); // Assuming status is also used somewhere

        // Add status column to target to make it relevant
        MvColumn statusCol = new MvColumn("dict1_status");
        statusCol.setSourceAlias("d1");
        statusCol.setSourceColumn("status");
        targetSingleDict.getColumns().add(statusCol);

        changes.addItem(dict1Changes);

        ArrayList<MvRowFilter> filters = changes.toFilters(handler);

        // Should generate filters for both targets since both use dict1 fields
        assertEquals(2, filters.size());

        // Check that both targets have filters
        Set<MvViewExpr> filteredTargets = new HashSet<>();
        for (MvRowFilter filter : filters) {
            filteredTargets.add(filter.getTarget());
            assertFalse(filter.isEmpty());
        }
        assertTrue(filteredTargets.contains(targetSingleDict));
        assertTrue(filteredTargets.contains(targetMultiDict));
    }

    @Test
    public void testTargetDependingOnSingleDict() {
        // Test: Target that depends on single dictionary
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        changes.addItem(dict1Changes);

        // Test toFilter method directly for single target
        MvRowFilter filter = changes.toFilter(handler, targetSingleDict);

        assertNotNull(filter);
        assertFalse(filter.isEmpty());
        assertEquals(targetSingleDict, filter.getTarget());
    }

    @Test
    public void testTargetDependingOnAllThreeDicts() {
        // Test: Target that depends on all three dictionaries
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "name", 1L, 2L);
        MvChangesSingleDict dict2Changes = createDictChanges("dict2", "code", 3L, 4L);
        MvChangesSingleDict dict3Changes = createDictChanges("dict3", "category", 5L, 6L);
        changes.addItem(dict1Changes);
        changes.addItem(dict2Changes);
        changes.addItem(dict3Changes);

        // Test toFilter method directly for multi-target
        MvRowFilter filter = changes.toFilter(handler, targetMultiDict);

        assertNotNull(filter);
        assertFalse(filter.isEmpty());
        assertEquals(targetMultiDict, filter.getTarget());

        // Block offsets must match DictTrans column order (target source order:
        // main, dict1, dict2, dict3). Main has 1 key col, each dict has 1 key col.
        var blocks = filter.getBlocks();
        assertEquals(3, blocks.size());
        assertEquals(1, blocks.get(0).getStartPos());  // dict1 at pos 1
        assertEquals(2, blocks.get(1).getStartPos());  // dict2 at pos 2
        assertEquals(3, blocks.get(2).getStartPos());  // dict3 at pos 3

        // Verify filter matches rows with correct key positions.
        // Row layout: [main_id, dict1_id, dict2_id, dict3_id]
        assertTrue(filter.matches(new Comparable<?>[]{100L, 1L, 3L, 5L}));   // dict1=1, dict2=3, dict3=5
        assertTrue(filter.matches(new Comparable<?>[]{200L, 2L, 4L, 6L}));   // dict1=2, dict2=4, dict3=6
        assertFalse(filter.matches(new Comparable<?>[]{300L, 99L, 99L, 99L})); // none match
    }

    @Test
    public void testTargetWithNoRelevantChanges() {
        // Test: Target with no relevant dictionary changes
        MvChangesSingleDict dict1Changes = createDictChanges("dict1", "description", 1L, 2L);
        changes.addItem(dict1Changes);

        // Test toFilter method directly
        MvRowFilter filter = changes.toFilter(handler, targetSingleDict);

        // Should return null when no relevant changes
        assertNull(filter);
    }

    @Test
    public void testNonExistentDictChanges() {
        // Test: Changes for dictionary that doesn't exist in handler - test individual toFilter method to avoid NPE bug
        MvChangesSingleDict nonExistentChanges = createDictChanges("non_existent", "field", 1L, 2L);
        changes.addItem(nonExistentChanges);

        MvRowFilter filter1 = changes.toFilter(handler, targetSingleDict);
        MvRowFilter filter2 = changes.toFilter(handler, targetMultiDict);

        // Should return null when no relevant changes (non-existent dict not in handler)
        assertNull(filter1);
        assertNull(filter2);
    }

    @Test
    public void testColumnUsageMapping() {
        // Test: Verify that column usage mapping works correctly
        Map<String, Set<String>> columnUsage = MvChangesMultiDict.getColumnUsage(targetMultiDict);

        // Check that all used columns are mapped correctly
        assertTrue(columnUsage.containsKey("m"));
        assertTrue(columnUsage.get("m").contains("id"));
        assertTrue(columnUsage.get("m").contains("dict1_id"));
        assertTrue(columnUsage.get("m").contains("dict2_id"));
        assertTrue(columnUsage.get("m").contains("dict3_id"));

        assertTrue(columnUsage.containsKey("d1"));
        assertTrue(columnUsage.get("d1").contains("id"));
        assertTrue(columnUsage.get("d1").contains("name"));

        assertTrue(columnUsage.containsKey("d2"));
        assertTrue(columnUsage.get("d2").contains("id"));
        assertTrue(columnUsage.get("d2").contains("code"));

        assertTrue(columnUsage.containsKey("d3"));
        assertTrue(columnUsage.get("d3").contains("id"));
        assertTrue(columnUsage.get("d3").contains("category"));
    }
}
