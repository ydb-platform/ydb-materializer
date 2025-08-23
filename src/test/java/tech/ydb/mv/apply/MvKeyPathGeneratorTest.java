package tech.ydb.mv.apply;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import tech.ydb.table.values.PrimitiveType;

import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvSqlPos;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;

/**
 * Test class for MvKeyPathGenerator
 *
 * @author zinal
 */
public class MvKeyPathGeneratorTest {

    private MvTarget originalTarget;
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

        // Create join conditions: A -> B -> C
        MvJoinCondition conditionAB = new MvJoinCondition(new MvSqlPos(2, 1));
        conditionAB.setFirstRef(sourceA);
        conditionAB.setFirstAlias("a");
        conditionAB.setFirstColumn("b_ref");
        conditionAB.setSecondRef(sourceB);
        conditionAB.setSecondAlias("b");
        conditionAB.setSecondColumn("id");
        sourceB.getConditions().add(conditionAB);

        MvJoinCondition conditionBC = new MvJoinCondition(new MvSqlPos(3, 1));
        conditionBC.setFirstRef(sourceB);
        conditionBC.setFirstAlias("b");
        conditionBC.setFirstColumn("id");
        conditionBC.setSecondRef(sourceC);
        conditionBC.setSecondAlias("c");
        conditionBC.setSecondColumn("b_id");
        sourceC.getConditions().add(conditionBC);

        MvJoinCondition conditionAD = new MvJoinCondition(new MvSqlPos(4, 1));
        conditionAD.setFirstRef(sourceA);
        conditionAD.setFirstAlias("a");
        conditionAD.setFirstColumn("id");
        conditionAD.setSecondRef(sourceD);
        conditionAD.setSecondAlias("d");
        conditionAD.setSecondColumn("a_id");
        sourceD.getConditions().add(conditionAD);

        // Create original target
        originalTarget = new MvTarget("test_target", new MvSqlPos(1, 1));
        originalTarget.getSources().add(sourceA);
        originalTarget.getSources().add(sourceB);
        originalTarget.getSources().add(sourceC);
        originalTarget.getSources().add(sourceD);

        // Add output columns for each join source's "id" column
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

        MvColumn columnD = new MvColumn("d_value", new MvSqlPos(3, 1));
        columnD.setSourceAlias("d");
        columnD.setSourceColumn("value");
        columnD.setSourceRef(sourceC);
        columnD.setType(PrimitiveType.Text);
        originalTarget.getColumns().add(columnD);

        if (! inputPrinted) {
            System.out.println("*** Input SQL: " + new MvSqlGen(originalTarget).makeCreateView());
            inputPrinted = true;
        }
    }

    @Test
    public void testGenerateKeyPath_DirectCase() {
        // Test case where input source is the top-most source
        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceA);

        assertNotNull(result);
        System.out.println("*** A-A SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(1, result.getSources().size());
        assertEquals("a", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());

        // Should have columns for primary key of sourceA (target primary key)
        assertEquals(1, result.getColumns().size());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("a", result.getColumns().get(0).getSourceAlias());
    }

    @Test
    public void testGenerateKeyPath_OneStep() {
        // Test transformation from B to A (optimized - B has a_id foreign key)
        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceB);

        assertNotNull(result);
        System.out.println("*** B-A SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(2, result.getSources().size());

        // Should have B as main source
        assertEquals("b", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());

        // Should also have A
        assertEquals("a", result.getSources().get(1).getTableAlias());
        assertEquals(MvJoinMode.INNER, result.getSources().get(1).getMode());

        // Should have columns for primary key of target (A), mapped from B's foreign key
        assertEquals(1, result.getColumns().size());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("a", result.getColumns().get(0).getSourceAlias());
        assertEquals("id", result.getColumns().get(0).getSourceColumn());
    }

    @Test
    public void testGenerateKeyPath_TwoSteps() {
        // Test transformation from C to A (two steps: C -> B -> A)
        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceC);

        assertNotNull(result);
        System.out.println("*** C-B-A SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(3, result.getSources().size());

        // Sources should be C (main), B, A
        assertEquals("c", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());
        assertEquals("b", result.getSources().get(1).getTableAlias());
        assertEquals("a", result.getSources().get(2).getTableAlias());

        // Should have columns for primary key of target (A)
        assertEquals(1, result.getColumns().size());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("a", result.getColumns().get(0).getSourceAlias());
    }

    @Test
    public void testGenerateKeyPath_Optimize() {
        // Test transformation from D to A (one step: D)
        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceD);

        assertNotNull(result);
        System.out.println("*** D-A SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(1, result.getSources().size());
        assertEquals("d", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());

        // Should have columns for primary key of sourceA (target primary key)
        assertEquals(1, result.getColumns().size());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("d", result.getColumns().get(0).getSourceAlias());
        assertEquals("a_id", result.getColumns().get(0).getSourceColumn());
    }

    @Test
    public void testGenerateKeyPath_NoPath() {
        // Create a disconnected source
        MvJoinSource disconnectedSource = new MvJoinSource(new MvSqlPos(4, 1));
        disconnectedSource.setTableName("tableX");
        disconnectedSource.setTableAlias("x");
        disconnectedSource.setMode(MvJoinMode.LEFT);

        MvTableInfo tableInfoX = MvTableInfo.newBuilder("tableX")
                .addColumn("id", PrimitiveType.Uint64)
                .addKey("id")
                .build();
        disconnectedSource.setTableInfo(tableInfoX);

        originalTarget.getSources().add(disconnectedSource);

        // Should return null when no path exists
        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(disconnectedSource);
        assertNull(result);
    }

    @Test
    public void testGenerateKeyPath_CircularReference() {
        // Create a circular reference: A -> B -> A
        MvJoinCondition circularCondition = new MvJoinCondition(new MvSqlPos(4, 1));
        circularCondition.setFirstRef(sourceD);
        circularCondition.setFirstAlias("d");
        circularCondition.setFirstColumn("id");
        circularCondition.setSecondRef(sourceA);
        circularCondition.setSecondAlias("a");
        circularCondition.setSecondColumn("id");
        sourceA.getConditions().add(circularCondition);

        // Should still optimize (D has a_id foreign key to A)
        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceD);

        assertNotNull(result);
        System.out.println("*** Circular SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(1, result.getSources().size()); // Optimized: only D needed
    }

    @Test
    public void testGenerateKeyPath_MultipleKeyColumns() {
        // Create table with composite primary key
        MvTableInfo tableInfoMultiKey = MvTableInfo.newBuilder("tableMultiKey")
                .addColumn("id1", PrimitiveType.Uint64)
                .addColumn("id2", PrimitiveType.Text)
                .addColumn("data", PrimitiveType.Text)
                .addKey("id1")
                .addKey("id2")
                .build();

        sourceA.setTableInfo(tableInfoMultiKey);

        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceA);

        assertNotNull(result);
        System.out.println("*** Multikey SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(2, result.getColumns().size());

        // Should have columns for both key fields
        boolean hasId1 = false, hasId2 = false;
        for (MvColumn column : result.getColumns()) {
            if ("id1".equals(column.getName())) {
                hasId1 = true;
            }
            if ("id2".equals(column.getName())) {
                hasId2 = true;
            }
        }
        assertTrue(hasId1 && hasId2);
    }

    @Test
    public void testGenerateKeyPath_LiteralCondition() {
        MvJoinCondition literalCond = new MvJoinCondition();
        literalCond.setFirstAlias(sourceB.getTableAlias());
        literalCond.setFirstRef(sourceB);
        literalCond.setFirstColumn("some");
        literalCond.setSecondLiteral(originalTarget.addLiteral("'AAA'"));
        sourceB.getConditions().add(literalCond);

        MvTarget result = new MvKeyPathGenerator(originalTarget).generate(sourceC);

        assertNotNull(result);
        System.out.println("*** Literal condition SQL: " + new MvSqlGen(result).makeSelect());
    }
}
