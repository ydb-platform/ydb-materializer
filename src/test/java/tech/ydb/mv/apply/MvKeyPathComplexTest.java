package tech.ydb.mv.apply;

import org.junit.jupiter.api.Test;
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
 * Complex test class for MvKeyPathGenerator
 *
 * @author zinal
 */
public class MvKeyPathComplexTest {

    @Test
    public void testGenerateKeyPath_MultiColumnJoinConditions() {
        // Create tables with composite join conditions
        MvTableInfo tableE = MvTableInfo.newBuilder("tableE")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("tenant_id", PrimitiveType.Uint64)
                .addColumn("user_id", PrimitiveType.Uint64)
                .addColumn("name", PrimitiveType.Text)
                .addKey("id")
                .build();

        MvTableInfo tableF = MvTableInfo.newBuilder("tableF")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("e_tenant_id", PrimitiveType.Uint64)
                .addColumn("e_user_id", PrimitiveType.Uint64)
                .addColumn("value", PrimitiveType.Text)
                .addKey("id")
                .build();

        // Create join sources
        MvJoinSource sourceE = new MvJoinSource();
        sourceE.setTableName("tableE");
        sourceE.setTableAlias("e");
        sourceE.setMode(MvJoinMode.MAIN);
        sourceE.setTableInfo(tableE);

        MvJoinSource sourceF = new MvJoinSource();
        sourceF.setTableName("tableF");
        sourceF.setTableAlias("f");
        sourceF.setMode(MvJoinMode.LEFT);
        sourceF.setTableInfo(tableF);

        // Create multi-column join condition: E(tenant_id, user_id) = F(e_tenant_id, e_user_id)
        MvJoinCondition conditionEF1 = new MvJoinCondition();
        conditionEF1.setFirstRef(sourceE);
        conditionEF1.setFirstAlias("e");
        conditionEF1.setFirstColumn("tenant_id");
        conditionEF1.setSecondRef(sourceF);
        conditionEF1.setSecondAlias("f");
        conditionEF1.setSecondColumn("e_tenant_id");
        sourceF.getConditions().add(conditionEF1);

        MvJoinCondition conditionEF2 = new MvJoinCondition();
        conditionEF2.setFirstRef(sourceE);
        conditionEF2.setFirstAlias("e");
        conditionEF2.setFirstColumn("user_id");
        conditionEF2.setSecondRef(sourceF);
        conditionEF2.setSecondAlias("f");
        conditionEF2.setSecondColumn("e_user_id");
        sourceF.getConditions().add(conditionEF2);

        // Create target with multi-column join
        MvTarget multiColTarget = new MvTarget("multi_col_target");
        multiColTarget.getSources().add(sourceE);
        multiColTarget.getSources().add(sourceF);

        // Add output columns
        MvColumn columnE = new MvColumn("e_name");
        columnE.setSourceAlias("e");
        columnE.setSourceColumn("name");
        columnE.setSourceRef(sourceE);
        columnE.setType(PrimitiveType.Text);
        multiColTarget.getColumns().add(columnE);

        MvColumn columnF = new MvColumn("f_value");
        columnF.setSourceAlias("f");
        columnF.setSourceColumn("value");
        columnF.setSourceRef(sourceF);
        columnF.setType(PrimitiveType.Text);
        multiColTarget.getColumns().add(columnF);

        // Test transformation from F to E
        MvTarget result = new MvKeyPathGenerator(multiColTarget).generate(sourceF);

        assertNotNull(result);
        System.out.println("*** Multi-column F-E SQL: " + new MvSqlGen(result).makeSelect());

        assertEquals(2, result.getSources().size());
        assertEquals("f", result.getSources().get(0).getTableAlias());
        assertEquals("e", result.getSources().get(1).getTableAlias());

        // Verify that both join conditions are copied
        MvJoinSource fSource = result.getSources().get(1); // sourceE in the result
        assertEquals(2, fSource.getConditions().size());

        // Check that the conditions cover both columns
        boolean hasTenantCondition = false;
        boolean hasUserCondition = false;
        for (MvJoinCondition condition : fSource.getConditions()) {
            if ("tenant_id".equals(condition.getFirstColumn()) && "e_tenant_id".equals(condition.getSecondColumn())) {
                hasTenantCondition = true;
            }
            if ("user_id".equals(condition.getFirstColumn()) && "e_user_id".equals(condition.getSecondColumn())) {
                hasUserCondition = true;
            }
        }
        assertTrue(hasTenantCondition, "Should have tenant_id join condition");
        assertTrue(hasUserCondition, "Should have user_id join condition");
    }

    @Test
    public void testGenerateKeyPath_LiteralJoinConditions() {
        // Create a 3-table scenario: G -> I -> H where I is an intermediate table
        // This will force a join scenario and test literal conditions properly
        MvTableInfo tableG = MvTableInfo.newBuilder("tableG")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("data", PrimitiveType.Text)
                .addKey("id")
                .build();

        MvTableInfo tableI = MvTableInfo.newBuilder("tableI")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("g_id", PrimitiveType.Uint64)
                .addColumn("category", PrimitiveType.Text)
                .addColumn("status", PrimitiveType.Text)
                .addColumn("type_code", PrimitiveType.Uint64)
                .addKey("id")
                .build();

        MvTableInfo tableH = MvTableInfo.newBuilder("tableH")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("i_id", PrimitiveType.Uint64)
                .addColumn("value", PrimitiveType.Text)
                .addKey("id")
                .build();

        // Create join sources: G -> I -> H
        MvJoinSource sourceG = new MvJoinSource();
        sourceG.setTableName("tableG");
        sourceG.setTableAlias("g");
        sourceG.setMode(MvJoinMode.MAIN);
        sourceG.setTableInfo(tableG);

        MvJoinSource sourceI = new MvJoinSource();
        sourceI.setTableName("tableI");
        sourceI.setTableAlias("i");
        sourceI.setMode(MvJoinMode.INNER);
        sourceI.setTableInfo(tableI);

        MvJoinSource sourceH = new MvJoinSource();
        sourceH.setTableName("tableH");
        sourceH.setTableAlias("h");
        sourceH.setMode(MvJoinMode.LEFT);
        sourceH.setTableInfo(tableH);

        // Create target with literal conditions
        MvTarget literalTarget = new MvTarget("literal_target");
        literalTarget.getSources().add(sourceG);
        literalTarget.getSources().add(sourceI);
        literalTarget.getSources().add(sourceH);

        // Create join conditions: G.id = I.g_id AND I.status = 'ACTIVE' AND I.type_code = 100
        MvJoinCondition conditionGI1 = new MvJoinCondition();
        conditionGI1.setFirstRef(sourceG);
        conditionGI1.setFirstAlias("g");
        conditionGI1.setFirstColumn("id");
        conditionGI1.setSecondRef(sourceI);
        conditionGI1.setSecondAlias("i");
        conditionGI1.setSecondColumn("g_id");
        sourceI.getConditions().add(conditionGI1);

        MvJoinCondition conditionGI2 = new MvJoinCondition();
        conditionGI2.setFirstRef(sourceI);
        conditionGI2.setFirstAlias("i");
        conditionGI2.setFirstColumn("status");
        conditionGI2.setSecondLiteral(literalTarget.addLiteral("'ACTIVE'"));
        sourceI.getConditions().add(conditionGI2);

        MvJoinCondition conditionGI3 = new MvJoinCondition();
        conditionGI3.setFirstRef(sourceI);
        conditionGI3.setFirstAlias("i");
        conditionGI3.setFirstColumn("type_code");
        conditionGI3.setSecondLiteral(literalTarget.addLiteral("100"));
        sourceI.getConditions().add(conditionGI3);

        // Create join condition: I.id = H.i_id
        MvJoinCondition conditionIH = new MvJoinCondition();
        conditionIH.setFirstRef(sourceI);
        conditionIH.setFirstAlias("i");
        conditionIH.setFirstColumn("id");
        conditionIH.setSecondRef(sourceH);
        conditionIH.setSecondAlias("h");
        conditionIH.setSecondColumn("i_id");
        sourceH.getConditions().add(conditionIH);

        // Add output columns
        MvColumn columnG = new MvColumn("g_data");
        columnG.setSourceAlias("g");
        columnG.setSourceColumn("data");
        columnG.setSourceRef(sourceG);
        columnG.setType(PrimitiveType.Text);
        literalTarget.getColumns().add(columnG);

        MvColumn columnI = new MvColumn("i_category");
        columnI.setSourceAlias("i");
        columnI.setSourceColumn("category");
        columnI.setSourceRef(sourceI);
        columnI.setType(PrimitiveType.Text);
        literalTarget.getColumns().add(columnI);

        MvColumn columnH = new MvColumn("h_value");
        columnH.setSourceAlias("h");
        columnH.setSourceColumn("value");
        columnH.setSourceRef(sourceH);
        columnH.setType(PrimitiveType.Text);
        literalTarget.getColumns().add(columnH);


        // Test transformation from H to G (should go H -> I -> G)
        MvTarget result = new MvKeyPathGenerator(literalTarget).generate(sourceH);
        System.out.println("*** Input H-I-G SQL: " + new MvSqlGen(literalTarget).makeSelect());
        System.out.println("*** Result H-I-G SQL: " + new MvSqlGen(result).makeSelect());

        assertNotNull(result);
        assertEquals(3, result.getSources().size());
        assertEquals("h", result.getSources().get(0).getTableAlias()); // Main source
        assertEquals("i", result.getSources().get(1).getTableAlias()); // First join
        assertEquals("g", result.getSources().get(2).getTableAlias()); // Second join

        // Verify I source has the correct join condition to H
        MvJoinSource iSourceInResult = result.getSources().get(1);
        assertEquals(3, iSourceInResult.getConditions().size());
        MvJoinCondition iCondition = iSourceInResult.getConditions().get(2);
        assertEquals("i", iCondition.getFirstAlias());
        assertEquals("id", iCondition.getFirstColumn());
        assertEquals("h", iCondition.getSecondAlias());
        assertEquals("i_id", iCondition.getSecondColumn());

        // Verify I source has the correct join conditions to G
        MvJoinSource gSourceInResult = result.getSources().get(2);
        assertEquals(1, gSourceInResult.getConditions().size());

        // Check that the structural join condition is present
        boolean hasStatusLiteralCondition = false;
        boolean hasTypeCodeLiteralCondition = false;

        for (MvJoinCondition condition : iSourceInResult.getConditions()) {
            if ("i".equals(condition.getFirstAlias()) && "status".equals(condition.getFirstColumn()) &&
                      condition.getSecondLiteral() != null && "'ACTIVE'".equals(condition.getSecondLiteral().getValue())) {
                hasStatusLiteralCondition = true;
            } else if ("i".equals(condition.getFirstAlias()) && "type_code".equals(condition.getFirstColumn()) &&
                      condition.getSecondLiteral() != null && "100".equals(condition.getSecondLiteral().getValue())) {
                hasTypeCodeLiteralCondition = true;
            }
        }

        assertTrue(hasStatusLiteralCondition, "Should have status literal condition");
        assertTrue(hasTypeCodeLiteralCondition, "Should have type_code literal condition");

        // The literal conditions are now properly copied because G is referenced in the
        // original target's output columns, and these conditions are essential for ensuring
        // the correct rows participate in the join relationships.
    }

    @Test
    public void testGenerateKeyPath_LiteralJoinConditions_NotReferenced() {
        // Test that literal conditions are NOT copied for sources that are NOT referenced in output
        MvTableInfo tableJ = MvTableInfo.newBuilder("tableJ")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("status", PrimitiveType.Text)
                .addColumn("name", PrimitiveType.Text)
                .addKey("id")
                .build();

        MvTableInfo tableK = MvTableInfo.newBuilder("tableK")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("j_id", PrimitiveType.Uint64)
                .addColumn("category", PrimitiveType.Text)
                .addKey("id")
                .build();

        // Create join sources: J -> K
        MvJoinSource sourceJ = new MvJoinSource();
        sourceJ.setTableName("tableJ");
        sourceJ.setTableAlias("j");
        sourceJ.setMode(MvJoinMode.MAIN);
        sourceJ.setTableInfo(tableJ);

        MvJoinSource sourceK = new MvJoinSource();
        sourceK.setTableName("tableK");
        sourceK.setTableAlias("k");
        sourceK.setMode(MvJoinMode.INNER);
        sourceK.setTableInfo(tableK);

        // Create target
        MvTarget notReferencedTarget = new MvTarget("not_referenced_target");
        notReferencedTarget.getSources().add(sourceJ);
        notReferencedTarget.getSources().add(sourceK);

        // Create join conditions: J.id = K.j_id AND J.status = 'ACTIVE'
        MvJoinCondition conditionJK1 = new MvJoinCondition();
        conditionJK1.setFirstRef(sourceJ);
        conditionJK1.setFirstAlias("j");
        conditionJK1.setFirstColumn("id");
        conditionJK1.setSecondRef(sourceK);
        conditionJK1.setSecondAlias("k");
        conditionJK1.setSecondColumn("j_id");
        sourceK.getConditions().add(conditionJK1);

        MvJoinCondition conditionJK2 = new MvJoinCondition();
        conditionJK2.setFirstRef(sourceJ);
        conditionJK2.setFirstAlias("j");
        conditionJK2.setFirstColumn("status");
        conditionJK2.setSecondLiteral(notReferencedTarget.addLiteral("'ACTIVE'"));
        sourceK.getConditions().add(conditionJK2);

        // Add output column that only references K (NOT J)
        MvColumn columnK = new MvColumn("k_category");
        columnK.setSourceAlias("k");
        columnK.setSourceColumn("category");
        columnK.setSourceRef(sourceK);
        columnK.setType(PrimitiveType.Text);
        notReferencedTarget.getColumns().add(columnK);

        // Test transformation from K to J
        MvTarget result = new MvKeyPathGenerator(notReferencedTarget).generate(sourceK);

        assertNotNull(result);
        // The generator optimizes this case to use only K with the foreign key mapping
        assertEquals(1, result.getSources().size());
        assertEquals("k", result.getSources().get(0).getTableAlias()); // Main (and only) source

        // Verify that K source has no literal conditions
        // The literal condition J.status = 'ACTIVE' should NOT be copied because
        // J is not referenced in the original target's output, and the optimization
        // doesn't require an explicit join to J
        MvJoinSource kSourceInResult = result.getSources().get(0);

        // K should have no conditions because:
        // 1. The structural condition J.id = K.j_id is not needed (optimization)
        // 2. The literal condition J.status = 'ACTIVE' should not be copied because J is not referenced
        assertEquals(0, kSourceInResult.getConditions().size());

        // Verify that the key mapping uses K's foreign key column
        assertEquals(1, result.getColumns().size());
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("k", result.getColumns().get(0).getSourceAlias());
        assertEquals("j_id", result.getColumns().get(0).getSourceColumn());
    }

    @Test
    public void testGenerateKeyPath_CrossTableJoinConditions() {
        // Create complex scenario where join conditions reference columns from different tables
        MvTableInfo tableX = MvTableInfo.newBuilder("tableX")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("org_id", PrimitiveType.Uint64)
                .addColumn("name", PrimitiveType.Text)
                .addKey("id")
                .build();

        MvTableInfo tableY = MvTableInfo.newBuilder("tableY")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("x_id", PrimitiveType.Uint64)
                .addColumn("category", PrimitiveType.Text)
                .addKey("id")
                .build();

        MvTableInfo tableZ = MvTableInfo.newBuilder("tableZ")
                .addColumn("id", PrimitiveType.Uint64)
                .addColumn("y_id", PrimitiveType.Uint64)
                .addColumn("x_org_id", PrimitiveType.Uint64) // References X.org_id
                .addColumn("value", PrimitiveType.Text)
                .addKey("id")
                .build();

        // Create join sources: X -> Y -> Z
        MvJoinSource sourceX = new MvJoinSource();
        sourceX.setTableName("tableX");
        sourceX.setTableAlias("x");
        sourceX.setMode(MvJoinMode.MAIN);
        sourceX.setTableInfo(tableX);

        MvJoinSource sourceY = new MvJoinSource();
        sourceY.setTableName("tableY");
        sourceY.setTableAlias("y");
        sourceY.setMode(MvJoinMode.INNER);
        sourceY.setTableInfo(tableY);

        MvJoinSource sourceZ = new MvJoinSource();
        sourceZ.setTableName("tableZ");
        sourceZ.setTableAlias("z");
        sourceZ.setMode(MvJoinMode.LEFT);
        sourceZ.setTableInfo(tableZ);

        // Create join conditions
        // Y joins to X: X.id = Y.x_id
        MvJoinCondition conditionXY = new MvJoinCondition();
        conditionXY.setFirstRef(sourceX);
        conditionXY.setFirstAlias("x");
        conditionXY.setFirstColumn("id");
        conditionXY.setSecondRef(sourceY);
        conditionXY.setSecondAlias("y");
        conditionXY.setSecondColumn("x_id");
        sourceY.getConditions().add(conditionXY);

        // Z joins to Y: Y.id = Z.y_id
        MvJoinCondition conditionYZ1 = new MvJoinCondition();
        conditionYZ1.setFirstRef(sourceY);
        conditionYZ1.setFirstAlias("y");
        conditionYZ1.setFirstColumn("id");
        conditionYZ1.setSecondRef(sourceZ);
        conditionYZ1.setSecondAlias("z");
        conditionYZ1.setSecondColumn("y_id");
        sourceZ.getConditions().add(conditionYZ1);

        // Z also joins directly to X: X.org_id = Z.x_org_id (cross-table condition)
        MvJoinCondition conditionXZ = new MvJoinCondition();
        conditionXZ.setFirstRef(sourceX);
        conditionXZ.setFirstAlias("x");
        conditionXZ.setFirstColumn("org_id");
        conditionXZ.setSecondRef(sourceZ);
        conditionXZ.setSecondAlias("z");
        conditionXZ.setSecondColumn("x_org_id");
        sourceZ.getConditions().add(conditionXZ);

        // Create target
        MvTarget crossTarget = new MvTarget("cross_target");
        crossTarget.getSources().add(sourceX);
        crossTarget.getSources().add(sourceY);
        crossTarget.getSources().add(sourceZ);

        // Add output columns
        MvColumn columnX = new MvColumn("x_name");
        columnX.setSourceAlias("x");
        columnX.setSourceColumn("name");
        columnX.setSourceRef(sourceX);
        columnX.setType(PrimitiveType.Text);
        crossTarget.getColumns().add(columnX);

        MvColumn columnY = new MvColumn("y_category");
        columnY.setSourceAlias("y");
        columnY.setSourceColumn("category");
        columnY.setSourceRef(sourceY);
        columnY.setType(PrimitiveType.Text);
        crossTarget.getColumns().add(columnY);

        MvColumn columnZ = new MvColumn("z_value");
        columnZ.setSourceAlias("z");
        columnZ.setSourceColumn("value");
        columnZ.setSourceRef(sourceZ);
        columnZ.setType(PrimitiveType.Text);
        crossTarget.getColumns().add(columnZ);

        // Test transformation from Z to X (should go Z -> Y -> X)
        MvTarget result = new MvKeyPathGenerator(crossTarget).generate(sourceZ);

        assertNotNull(result);
        System.out.println("*** Cross-table Z-Y-X SQL: " + new MvSqlGen(result).makeSelect());

        // The generator optimizes this case! Instead of Z -> Y -> X, it uses the direct
        // cross-table relationship Z -> X through the condition X.org_id = Z.x_org_id
        assertEquals(2, result.getSources().size());
        assertEquals("z", result.getSources().get(0).getTableAlias()); // Main source
        assertEquals("x", result.getSources().get(1).getTableAlias()); // Direct join to target

        // Verify X source has the correct cross-table join condition to Z
        MvJoinSource xSourceInResult = result.getSources().get(1);
        assertEquals(1, xSourceInResult.getConditions().size());
        MvJoinCondition xCondition = xSourceInResult.getConditions().get(0);
        assertEquals("x", xCondition.getFirstAlias());
        assertEquals("org_id", xCondition.getFirstColumn());
        assertEquals("z", xCondition.getSecondAlias());
        assertEquals("x_org_id", xCondition.getSecondColumn());

        // Verify that the optimization worked correctly - direct path to target
        assertEquals(1, result.getColumns().size()); // Should have target key column
        assertEquals("id", result.getColumns().get(0).getName());
        assertEquals("x", result.getColumns().get(0).getSourceAlias());
    }
}
