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
import tech.ydb.mv.model.MvTarget;

import java.util.Arrays;

/**
 * Test class for MvFieldPathGenerator
 *
 * @author zinal
 */
public class MvFieldPathGeneratorTest {

    private static final boolean PRINT_SQL = SqlConstants.PRINT_SQL;

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
        originalTarget = new MvTarget("test_target", new MvSqlPos(1, 1));
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
    public void testGenerateFields_DirectCase() {
        // Test case where target source is the top-most source
        MvTarget result = new MvKeyPathGenerator(originalTarget).extractFields(
                sourceA, Arrays.asList("name", "id"));
        assertNotNull(result);

        if (PRINT_SQL) {
            System.out.println("*** A fields SQL: " + new MvSqlGen(result).makeSelect());
        }

        assertEquals(1, result.getSources().size());
        assertEquals("a", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());

        // Should have columns for requested fields
        assertEquals(2, result.getColumns().size());
        boolean hasName = false, hasId = false;
        for (MvColumn column : result.getColumns()) {
            if ("name".equals(column.getName()) && "a".equals(column.getSourceAlias())) {
                hasName = true;
            }
            if ("id".equals(column.getName()) && "a".equals(column.getSourceAlias())) {
                hasId = true;
            }
        }
        assertTrue(hasName && hasId);
    }

    @Test
    public void testGenerateFields_OneStep() {
        // Test transformation to get fields from B (one step: A -> B)
        MvTarget result = new MvKeyPathGenerator(originalTarget).extractFields(
                sourceB, Arrays.asList("description", "some"));
        assertNotNull(result);

        if (PRINT_SQL) {
            System.out.println("*** B fields SQL: " + new MvSqlGen(result).makeSelect());
        }

        assertEquals(2, result.getSources().size());

        // Sources should be A (main), B
        assertEquals("a", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());
        assertEquals("b", result.getSources().get(1).getTableAlias());

        // Should have columns for requested fields from B
        assertEquals(2, result.getColumns().size());
        boolean hasDescription = false, hasSome = false;
        for (MvColumn column : result.getColumns()) {
            if ("description".equals(column.getName()) && "b".equals(column.getSourceAlias())) {
                hasDescription = true;
            }
            if ("some".equals(column.getName()) && "b".equals(column.getSourceAlias())) {
                hasSome = true;
            }
        }
        assertTrue(hasDescription && hasSome);
    }

    @Test
    public void testGenerateFields_TwoSteps() {
        // Test transformation to get fields from C (two steps: A -> B -> C)
        MvTarget result = new MvKeyPathGenerator(originalTarget).extractFields(
                sourceC, Arrays.asList("value"));
        assertNotNull(result);

        if (PRINT_SQL) {
            System.out.println("*** C fields SQL: " + new MvSqlGen(result).makeSelect());
        }

        assertEquals(3, result.getSources().size());

        // Sources should be A (main), B, C
        assertEquals("a", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());
        assertEquals("b", result.getSources().get(1).getTableAlias());
        assertEquals("c", result.getSources().get(2).getTableAlias());

        // Should have columns for requested fields from C
        assertEquals(1, result.getColumns().size());
        assertEquals("value", result.getColumns().get(0).getName());
        assertEquals("c", result.getColumns().get(0).getSourceAlias());
    }

    @Test
    public void testGenerateAllFields() {
        // Test transformation to get all fields from D
        MvTarget result = new MvKeyPathGenerator(originalTarget).extractFields(sourceD);
        assertNotNull(result);

        if (PRINT_SQL) {
            System.out.println("*** D all fields SQL: " + new MvSqlGen(result).makeSelect());
        }

        assertEquals(2, result.getSources().size());

        // Sources should be A (main), D
        assertEquals("a", result.getSources().get(0).getTableAlias());
        assertEquals(MvJoinMode.MAIN, result.getSources().get(0).getMode());
        assertEquals("d", result.getSources().get(1).getTableAlias());

        // Should have columns for all fields from D (id, a_id, value)
        assertEquals(3, result.getColumns().size());
        boolean hasId = false, hasAId = false, hasValue = false;
        for (MvColumn column : result.getColumns()) {
            if ("id".equals(column.getName()) && "d".equals(column.getSourceAlias())) {
                hasId = true;
            }
            if ("a_id".equals(column.getName()) && "d".equals(column.getSourceAlias())) {
                hasAId = true;
            }
            if ("value".equals(column.getName()) && "d".equals(column.getSourceAlias())) {
                hasValue = true;
            }
        }
        assertTrue(hasId && hasAId && hasValue);
    }

    @Test
    public void testGenerateFields_InvalidFieldName() {
        // Test with invalid field name
        assertThrows(IllegalArgumentException.class, () -> {
            new MvKeyPathGenerator(originalTarget).extractFields(sourceA, Arrays.asList("nonexistent"));
        });
    }

    @Test
    public void testGenerateFields_EmptyFieldList() {
        // Test with empty field list
        assertThrows(IllegalArgumentException.class, () -> {
            new MvKeyPathGenerator(originalTarget).extractFields(sourceA, Arrays.<String>asList());
        });
    }

    @Test
    public void testGenerateFields_NullParameters() {
        // Test with null parameters
        assertThrows(IllegalArgumentException.class, () -> {
            new MvKeyPathGenerator(originalTarget).extractFields(null, Arrays.asList("field"));
        });

        assertThrows(IllegalArgumentException.class, () -> {
            new MvKeyPathGenerator(originalTarget).extractFields(sourceA, null);
        });
    }

    @Test
    public void testGenerateFields_NoPath() {
        // Create a disconnected source
        MvJoinSource sourceE = new MvJoinSource(new MvSqlPos(5, 1));
        sourceE.setTableName("tableE");
        sourceE.setTableAlias("e");
        sourceE.setMode(MvJoinMode.LEFT);

        MvTableInfo tableInfoE = MvTableInfo.newBuilder("tableE")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("data", PrimitiveType.Text)
                .addKey("id")
                .build();
        sourceE.setTableInfo(tableInfoE);

        originalTarget.getSources().add(sourceE);

        // Should return null when no path exists
        MvTarget result = new MvKeyPathGenerator(originalTarget).extractFields(
                sourceE, Arrays.asList("data"));
        assertNull(result);
    }
}
