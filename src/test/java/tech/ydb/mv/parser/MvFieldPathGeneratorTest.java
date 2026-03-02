package tech.ydb.mv.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import tech.ydb.table.values.PrimitiveType;

import tech.ydb.mv.SqlConstants;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvSqlPos;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvViewExpr;

/**
 * Test class for MvFieldPathGenerator
 *
 * @author zinal
 */
public class MvFieldPathGeneratorTest {

    private static final boolean PRINT_SQL = SqlConstants.PRINT_SQL;

    private MvViewExpr originalTarget;
    private MvJoinSource sourceA, sourceB, sourceC, sourceD;
    private MvTableInfo tableInfoA, tableInfoB, tableInfoC, tableInfoD;
    private static volatile boolean inputPrinted = false;

    @BeforeEach
    public void setUp() {
        // Create table infos with primary keys
        tableInfoA = MvTableInfo.newBuilder("tableA")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("name", PrimitiveType.Text)
                .addColumn("b_ref", PrimitiveType.Uint64)
                .addKey("id")
                .build();

        tableInfoB = MvTableInfo.newBuilder("tableB")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("a_id", PrimitiveType.Uint64)
                .addColumn("some", PrimitiveType.Text)
                .addColumn("description", PrimitiveType.Text)
                .addKey("id")
                .build();

        tableInfoC = MvTableInfo.newBuilder("tableC")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("b_id", PrimitiveType.Uint64)
                .addColumn("clazz", PrimitiveType.Text)
                .addColumn("value", PrimitiveType.Text)
                .addKey("id")
                .build();

        tableInfoD = MvTableInfo.newBuilder("tableD")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("a_id", PrimitiveType.Uint64)
                .addColumn("value", PrimitiveType.Text)
                .addKey("id")
                .build();

        // Create join sources
        sourceA = new MvJoinSource(new MvSqlPos(1, 1));
        sourceA.setTableName("tableA");
        sourceA.setTableAlias("a");
        sourceA.setMode(MvJoinMode.MAIN);
        sourceA.setTableInfo(tableInfoA);

        sourceB = new MvJoinSource(new MvSqlPos(2, 1));
        sourceB.setTableName("tableB");
        sourceB.setTableAlias("b");
        sourceB.setMode(MvJoinMode.INNER);
        sourceB.setTableInfo(tableInfoB);

        sourceC = new MvJoinSource(new MvSqlPos(3, 1));
        sourceC.setTableName("tableC");
        sourceC.setTableAlias("c");
        sourceC.setMode(MvJoinMode.LEFT);
        sourceC.setTableInfo(tableInfoC);

        sourceD = new MvJoinSource(new MvSqlPos(4, 1));
        sourceD.setTableName("tableD");
        sourceD.setTableAlias("d");
        sourceD.setMode(MvJoinMode.LEFT);
        sourceD.setTableInfo(tableInfoD);

        // Create original target
        originalTarget = new MvViewExpr("test_target");
        originalTarget.setTableInfo(tableInfoA);

        // Add sources
        originalTarget.getSources().add(sourceA);
        originalTarget.getSources().add(sourceB);
        originalTarget.getSources().add(sourceC);
        originalTarget.getSources().add(sourceD);

        // Add join conditions: A -> B
        MvJoinCondition condAB = new MvJoinCondition();
        condAB.setFirstRef(sourceA);
        condAB.setFirstAlias("a");
        condAB.setFirstColumn("b_ref");
        condAB.setSecondRef(sourceB);
        condAB.setSecondAlias("b");
        condAB.setSecondColumn("a_id");
        sourceB.getConditions().add(condAB);

        // Add join conditions: B -> C
        MvJoinCondition condBC1 = new MvJoinCondition();
        condBC1.setFirstRef(sourceB);
        condBC1.setFirstAlias("b");
        condBC1.setFirstColumn("id");
        condBC1.setSecondRef(sourceC);
        condBC1.setSecondAlias("c");
        condBC1.setSecondColumn("b_id");
        sourceC.getConditions().add(condBC1);
        MvJoinCondition condBC2 = new MvJoinCondition();
        condBC2.setFirstLiteral(originalTarget.addLiteral("'LITERAL'u"));
        condBC2.setSecondRef(sourceC);
        condBC2.setSecondAlias("c");
        condBC2.setSecondColumn("clazz");
        sourceC.getConditions().add(condBC2);

        // Add join conditions: A -> D (direct connection)
        MvJoinCondition condAD = new MvJoinCondition();
        condAD.setFirstRef(sourceA);
        condAD.setFirstAlias("a");
        condAD.setFirstColumn("id");
        condAD.setSecondRef(sourceD);
        condAD.setSecondAlias("d");
        condAD.setSecondColumn("a_id");
        sourceD.getConditions().add(condAD);

        // Add some output columns
        MvColumn columnA = new MvColumn("a_name", new MvSqlPos(1, 1));
        columnA.setSourceAlias("a");
        columnA.setSourceColumn("name");
        columnA.setSourceRef(sourceA);
        columnA.setType(PrimitiveType.Text);
        originalTarget.getColumns().add(columnA);

        MvColumn columnB = new MvColumn("b_description", new MvSqlPos(2, 1));
        columnB.setSourceAlias("b");
        columnB.setSourceColumn("description");
        columnB.setSourceRef(sourceB);
        columnB.setType(PrimitiveType.Text);
        originalTarget.getColumns().add(columnB);

        MvColumn columnC = new MvColumn("c_value", new MvSqlPos(3, 1));
        columnC.setSourceAlias("c");
        columnC.setSourceColumn("value");
        columnC.setSourceRef(sourceC);
        columnC.setType(PrimitiveType.Text);
        originalTarget.getColumns().add(columnC);

        MvColumn columnD = new MvColumn("d_value", new MvSqlPos(4, 1));
        columnD.setSourceAlias("d");
        columnD.setSourceColumn("value");
        columnD.setSourceRef(sourceD);
        columnD.setType(PrimitiveType.Text);
        originalTarget.getColumns().add(columnD);

        if (PRINT_SQL) {
            if (!inputPrinted) {
                System.out.println("*** View SQL: " + new MvSqlGen(originalTarget).makeCreateView());
                inputPrinted = true;
            }
        }
    }

    @Test
    public void testApplyFilter() {
        var filter = MvPathGenerator.newFilter()
                .add(sourceA, "id")
                .add(sourceB, "id", "a_id")
                .add(sourceD, "id", "a_id");
        MvViewExpr result = new MvPathGenerator(originalTarget).applyFilter(filter);
        assertNotNull(result);

        if (PRINT_SQL) {
            System.out.println("*** Filtered SQL: " + new MvSqlGen(result).makeSelect());
        }

        assertEquals(3, result.getSources().size());
        assertEquals("a", result.getSources().get(0).getTableAlias());
        assertEquals("b", result.getSources().get(1).getTableAlias());
        assertEquals("d", result.getSources().get(2).getTableAlias());
        assertEquals(0, result.getSources().get(0).getConditions().size());
        assertEquals(1, result.getSources().get(1).getConditions().size());
        assertEquals(1, result.getSources().get(2).getConditions().size());
        assertEquals(5, result.getColumns().size());
    }

    /**
     * Verifies that applyFilter() produces columns in the same order as defined
     * by the sequence of Filter.add() calls. The ordering is critical for
     * ActionKeysFilter which expects column positions to match dictionary blocks.
     */
    @Test
    public void testApplyFilterColumnOrderingMatchesFilterAddSequence() {
        // Filter: add(sourceA, "id"), add(sourceB, "id", "a_id"), add(sourceD, "id", "a_id")
        // Expected column order: a.id, b.id, b.a_id, d.id, d.a_id
        var filter = MvPathGenerator.newFilter()
                .add(sourceA, "id")
                .add(sourceB, "id", "a_id")
                .add(sourceD, "id", "a_id");

        MvViewExpr result = new MvPathGenerator(originalTarget).applyFilter(filter);

        var columns = result.getColumns();
        assertEquals(5, columns.size());

        assertColumnAt(columns, 0, "a", "id");
        assertColumnAt(columns, 1, "b", "id");
        assertColumnAt(columns, 2, "b", "a_id");
        assertColumnAt(columns, 3, "d", "id");
        assertColumnAt(columns, 4, "d", "a_id");

        // Verify generated SQL lists columns in the same order
        String sql = new MvSqlGen(result).makeSelect();
        assertSqlColumnOrder(sql, "a.id", "b.id", "b.a_id", "d.id", "d.a_id");
    }

    /**
     * Verifies column ordering when add() calls are interleaved across sources.
     * Order must follow the exact sequence of column names passed to add().
     */
    @Test
    public void testApplyFilterColumnOrderingInterleavedSources() {
        // Interleaved: a.id, d.id, b.id, b.a_id, a.name
        var filter = MvPathGenerator.newFilter()
                .add(sourceA, "id")
                .add(sourceD, "id")
                .add(sourceB, "id", "a_id")
                .add(sourceA, "name");

        MvViewExpr result = new MvPathGenerator(originalTarget).applyFilter(filter);

        var columns = result.getColumns();
        assertEquals(5, columns.size());

        assertColumnAt(columns, 0, "a", "id");
        assertColumnAt(columns, 1, "d", "id");
        assertColumnAt(columns, 2, "b", "id");
        assertColumnAt(columns, 3, "b", "a_id");
        assertColumnAt(columns, 4, "a", "name");

        String sql = new MvSqlGen(result).makeSelect();
        assertSqlColumnOrder(sql, "a.id", "d.id", "b.id", "b.a_id", "a.name");
    }

    /**
     * Verifies column ordering with single source and multiple columns.
     */
    @Test
    public void testApplyFilterColumnOrderingSingleSourceMultipleColumns() {
        var filter = MvPathGenerator.newFilter()
                .add(sourceB, "id", "a_id", "some", "description");

        MvViewExpr result = new MvPathGenerator(originalTarget).applyFilter(filter);

        var columns = result.getColumns();
        assertEquals(4, columns.size());

        assertColumnAt(columns, 0, "b", "id");
        assertColumnAt(columns, 1, "b", "a_id");
        assertColumnAt(columns, 2, "b", "some");
        assertColumnAt(columns, 3, "b", "description");

        String sql = new MvSqlGen(result).makeSelect();
        assertSqlColumnOrder(sql, "b.id", "b.a_id", "b.some", "b.description");
    }

    private static void assertColumnAt(java.util.List<MvColumn> columns, int index,
            String expectedAlias, String expectedSourceColumn) {
        MvColumn col = columns.get(index);
        assertEquals(expectedAlias, col.getSourceAlias(),
                "Column at index " + index + " should have alias " + expectedAlias);
        assertEquals(expectedSourceColumn, col.getSourceColumn(),
                "Column at index " + index + " should have sourceColumn " + expectedSourceColumn);
    }

    private static void assertSqlColumnOrder(String sql, String... expectedColumnRefs) {
        // SQL format: "alias.column AS name" - extract SELECT clause and check column order
        int selectStart = sql.indexOf("SELECT");
        assertTrue(selectStart >= 0, "SQL should contain SELECT");
        int fromStart = sql.indexOf("FROM", selectStart);
        assertTrue(fromStart >= 0, "SQL should contain FROM");
        String selectClause = sql.substring(selectStart + 6, fromStart);

        String[] parts = selectClause.split(",");
        assertEquals(expectedColumnRefs.length, parts.length,
                "Number of columns in SELECT should match expected");

        for (int i = 0; i < expectedColumnRefs.length; i++) {
            String part = parts[i].trim();
            // Extract "alias.column" from "alias.column AS cN" (or "`alias`.`column` AS `cN`")
            int asIdx = part.indexOf(" AS ");
            String columnRef = asIdx >= 0 ? part.substring(0, asIdx).trim() : part;
            String normalized = columnRef.replace("`", "");
            String expected = expectedColumnRefs[i].replace("`", "");
            assertTrue(normalized.equals(expected),
                    "Column at position " + i + " should be " + expected + ", got: " + normalized);
        }
    }
}
