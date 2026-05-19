package tech.ydb.mv.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.SqlConstants;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.table.values.PrimitiveType;

/**
 * Test for MvSqlGen.makeSelect(), makePlainUpsert(), makePlainDelete(), and
 * makeConvertKeyToTarget() methods.
 *
 * @author zinal
 */
public class SqlGenSelectUpsertTest {

    private static final boolean PRINT_SQL = SqlConstants.PRINT_SQL;

    @Test
    public void testMakeSelect1() {
        // Parse the input SQL
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_GOOD1).fill();

        // Verify parsing was successful
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(1, mc.getViews().size());

        // Get the target and add MvTableInfo instances
        var view = mc.getViews().values().iterator().next();
        var target = view.getParts().values().iterator().next();
        addTableInfoToTarget(target);

        // Generate SQL
        MvSqlGen sqlGen = new MvSqlGen(target);
        String generatedSql = sqlGen.makeSelect();

        if (PRINT_SQL) {
            System.out.println("Generated SELECT SQL:");
            System.out.println(generatedSql);
        }

        // Validate the generated SQL structure
        validateGeneratedSelectSql1(generatedSql, target);
    }

    @Test
    public void testMakeSelect2() {
        // Parse the input SQL
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_GOOD2).fill();

        // Verify parsing was successful
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(1, mc.getViews().size());

        // Get the target and add MvTableInfo instances
        var view = mc.getViews().values().iterator().next();
        var target = view.getParts().values().iterator().next();
        addTableInfoToTarget2(target);

        // Generate SQL
        MvSqlGen sqlGen = new MvSqlGen(target);
        String generatedSql = sqlGen.makeSelect();

        if (PRINT_SQL) {
            System.out.println("Generated SELECT SQL for SQL_GOOD2:");
            System.out.println(generatedSql);
        }

        // Validate the generated SQL structure
        validateGeneratedSelectSql2(generatedSql, target);
    }

    @Test
    public void testMakePlainUpsert1() {
        MvViewExpr target = parseGood1Target();
        MvTableInfo targetInfo = SqlConstants.tiTarget("m1");
        addTargetTableInfo(target, targetInfo);

        String generatedSql = new MvSqlGen(target).makePlainUpsert();

        Assertions.assertTrue(generatedSql.startsWith("DECLARE " + MvSqlGen.SYS_INPUT_VAR + " AS List<Struct<"),
                "UPSERT should declare input rows");
        Assertions.assertTrue(generatedSql.contains("id:Int32"),
                "Input declaration should contain target key type");
        Assertions.assertTrue(generatedSql.contains("c10:Text"),
                "Input declaration should contain target text fields");
        Assertions.assertTrue(generatedSql.contains("UPSERT INTO m1" + MvSqlGen.EOL),
                "UPSERT should target the materialized view");
        Assertions.assertTrue(generatedSql.contains("SELECT * FROM AS_TABLE(" + MvSqlGen.SYS_INPUT_VAR + ");"),
                "UPSERT should write rows from the sys_input table parameter");
        Assertions.assertFalse(generatedSql.contains(MvSqlGen.SYS_KEYS_VAR),
                "Plain UPSERT should not declare key-only input");
    }

    @Test
    public void testMakePlainDeleteUsesSourceKeyForDirectDestinationKey() {
        MvViewExpr target = parseGood1Target();
        addTargetTableInfo(target, SqlConstants.tiTarget("m1"));

        String generatedSql = new MvSqlGen(target).makePlainDelete();

        Assertions.assertTrue(generatedSql.startsWith("DECLARE " + MvSqlGen.SYS_KEYS_VAR
                + " AS List<Struct<id:Int32>>;"),
                "Direct destination keys should use the top-most source key type");
        Assertions.assertTrue(generatedSql.contains("DELETE FROM m1" + MvSqlGen.EOL));
        Assertions.assertTrue(generatedSql.contains("ON SELECT * FROM AS_TABLE(" + MvSqlGen.SYS_KEYS_VAR + ");"));
    }

    @Test
    public void testMakePlainDeleteUsesDestinationKeyWhenKeysDiffer() {
        MvViewExpr target = parseGood1Target();
        addTargetTableInfo(target, MvTableInfo.newBuilder("m1")
                .addColumn("c8", tech.ydb.table.values.PrimitiveType.Text)
                .addKey("c8")
                .build());

        String generatedSql = new MvSqlGen(target).makePlainDelete();

        Assertions.assertTrue(generatedSql.startsWith("DECLARE " + MvSqlGen.SYS_KEYS_VAR
                + " AS List<Struct<c8:Text>>;"),
                "Non-direct destination keys should use the target table key type");
        Assertions.assertTrue(generatedSql.contains("DELETE FROM m1" + MvSqlGen.EOL));
    }

    @Test
    public void testMakeConvertKeyToTargetForDirectKeyMapping() {
        MvViewExpr target = parseGood1Target();
        addTargetTableInfo(target, SqlConstants.tiTarget("m1"));

        String generatedSql = new MvSqlGen(target).makeConvertKeyToTarget();

        Assertions.assertNotNull(generatedSql);
        Assertions.assertTrue(generatedSql.startsWith("DECLARE " + MvSqlGen.SYS_KEYS_VAR
                + " AS List<Struct<id:Int32>>;"));
        Assertions.assertTrue(generatedSql.contains("SELECT main.id AS id FROM AS_TABLE("
                + MvSqlGen.SYS_KEYS_VAR + ") AS main"));
    }

    @Test
    public void testMakeConvertKeyToTargetReturnsNullForNonTopmostKey() {
        MvViewExpr target = parseGood1Target();
        addTargetTableInfo(target, MvTableInfo.newBuilder("m1")
                .addColumn("c8", tech.ydb.table.values.PrimitiveType.Text)
                .addKey("c8")
                .build());

        Assertions.assertNull(new MvSqlGen(target).makeConvertKeyToTarget(),
                "Key conversion should be unavailable when the destination key is not mapped from the top source key");
    }

    @Test
    public void testDestKeyDirectRequiresActualTopmostKeyMapping() {
        MvViewExpr target = new MvViewExpr("mv_mismapped_key");

        MvJoinSource main = new MvJoinSource();
        main.setTableName("main_table");
        main.setTableAlias("main");
        main.setMode(MvJoinMode.MAIN);
        main.setTableInfo(MvTableInfo.newBuilder("main_table")
                .addColumn("id", PrimitiveType.Int32)
                .addColumn("sub_id", PrimitiveType.Int32)
                .addKey("id")
                .build());
        target.getSources().add(main);

        MvJoinSource sub = new MvJoinSource();
        sub.setTableName("sub_table");
        sub.setTableAlias("sub");
        sub.setMode(MvJoinMode.INNER);
        sub.setTableInfo(MvTableInfo.newBuilder("sub_table")
                .addColumn("id", PrimitiveType.Int32)
                .addKey("id")
                .build());
        target.getSources().add(sub);

        MvColumn id = new MvColumn("id");
        id.setSourceAlias("sub");
        id.setSourceColumn("id");
        id.setSourceRef(sub);
        id.setType(PrimitiveType.Int32);
        target.getColumns().add(id);

        target.setTableInfo(MvTableInfo.newBuilder("mv_mismapped_key")
                .addColumn("id", PrimitiveType.Int32)
                .addKey("id")
                .build());

        Assertions.assertFalse(target.isDestKeyDirect(),
                "Matching key names and types are not enough when the output key comes from a joined table");
        Assertions.assertNull(new MvSqlGen(target).makeConvertKeyToTarget(),
                "Key conversion should still be unavailable for a secondary-table key");
    }

    @Test
    public void testComputedTopmostKeyUsesKeyConversionForDelete() {
        MvViewExpr target = new MvViewExpr("mv_computed_key");

        MvJoinSource main = new MvJoinSource();
        main.setTableName("main_table");
        main.setTableAlias("main");
        main.setMode(MvJoinMode.MAIN);
        main.setTableInfo(MvTableInfo.newBuilder("main_table")
                .addColumn("id", PrimitiveType.Int32)
                .addKey("id")
                .build());
        target.getSources().add(main);

        MvColumn id = new MvColumn("id");
        id.setComputation(new MvComputation("main.id + 1000").addSource(main, "id"));
        id.setType(PrimitiveType.Int32);
        target.getColumns().add(id);

        target.setTableInfo(MvTableInfo.newBuilder("mv_computed_key")
                .addColumn("id", PrimitiveType.Int32)
                .addKey("id")
                .build());

        Assertions.assertFalse(target.isDestKeyDirect(),
                "Computed keys cannot reuse the source CDC key unchanged");
        String convertSql = new MvSqlGen(target).makeConvertKeyToTarget();
        Assertions.assertNotNull(convertSql,
                "Computed keys from topmost source keys should be converted before DELETE");
        Assertions.assertTrue(convertSql.contains("main.id + 1000 AS id"));
    }

    private MvViewExpr parseGood1Target() {
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_GOOD1).fill();
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(1, mc.getViews().size());
        var view = mc.getViews().values().iterator().next();
        var target = view.getParts().values().iterator().next();
        addTableInfoToTarget(target);
        return target;
    }

    private void addTargetTableInfo(MvViewExpr target, MvTableInfo targetInfo) {
        target.setTableInfo(targetInfo);
        for (var column : target.getColumns()) {
            var type = targetInfo.getColumns().get(column.getName());
            if (type != null) {
                column.setType(type);
            }
        }
    }

    private void addTableInfoToTarget(MvViewExpr target) {
        target.getSources().get(0).setTableInfo(
                SqlConstants.tiMainTable("main_table")
        );
        target.getSources().get(1).setTableInfo(
                SqlConstants.tiSubTable1("sub_table1")
        );
        target.getSources().get(2).setTableInfo(
                SqlConstants.tiSubTable2("sub_table2")
        );
        target.getSources().get(3).setTableInfo(
                SqlConstants.tiSubTable3("sub_table3")
        );
    }

    private void addTableInfoToTarget2(MvViewExpr target) {
        target.getSources().get(0).setTableInfo(
                SqlConstants.tiMainTable("schema3/main_table")
        );
        target.getSources().get(1).setTableInfo(
                SqlConstants.tiSubTable1("schema3/sub_table1")
        );
        target.getSources().get(2).setTableInfo(
                SqlConstants.tiSubTable2("schema3/sub_table2")
        );
        target.getSources().get(3).setTableInfo(
                SqlConstants.tiSubTable3("schema3/sub_table3")
        );
    }

    private void validateGeneratedSelectSql1(String sql, MvViewExpr target) {
        // Check for DECLARE statement
        Assertions.assertTrue(sql.startsWith("DECLARE $"),
                "SQL should start with DECLARE statement");
        Assertions.assertTrue(sql.contains("DECLARE " + MvSqlGen.SYS_KEYS_VAR + " AS "),
                "SQL should declare sys_keys parameter");

        // Check for List<Struct<...>> type declaration
        Assertions.assertTrue(sql.contains("List<Struct<"),
                "SQL should declare List<Struct<...>> type");

        // Check for primary key fields in type declaration
        Assertions.assertTrue(sql.contains("id:Int32"),
                "Type declaration should include id field");

        // Check for AS_TABLE subquery
        Assertions.assertTrue(sql.contains("AS_TABLE(" + MvSqlGen.SYS_KEYS_VAR + ") AS " + MvSqlGen.SYS_KEYS),
                "SQL should contain AS_TABLE subquery for sys_keys");

        // Check for INNER JOIN with main table
        Assertions.assertTrue(sql.contains("INNER JOIN"),
                "SQL should contain INNER JOIN with main table");

        // Check for ON condition with primary key
        Assertions.assertTrue(sql.contains("ON " + MvSqlGen.SYS_KEYS + ".id = main.id"),
                "SQL should contain ON condition for primary key");

        // Check for SELECT clause
        Assertions.assertTrue(sql.contains("SELECT"),
                "SQL should contain SELECT clause");

        // Check that all columns are present
        for (var column : target.getColumns()) {
            Assertions.assertTrue(sql.contains(" AS " + column.getName()),
                    "SQL should contain column: " + column.getName());
        }

        // Check for constants subquery (since we have literals in join conditions)
        Assertions.assertTrue(!target.getLiterals().isEmpty(),
                "Target should have literals");
        Assertions.assertTrue(sql.contains("FROM (SELECT"),
                "SQL should contain constants subquery");
        Assertions.assertTrue(sql.contains(") AS " + MvSqlGen.SYS_CONST),
                "SQL should have constants alias");

        // Check for CROSS JOIN with main table
        Assertions.assertTrue(sql.contains("CROSS JOIN"),
                "SQL should contain CROSS JOIN for main table");

        // Check for other joins
        Assertions.assertTrue(sql.contains("INNER JOIN"),
                "SQL should contain INNER JOIN");
        Assertions.assertTrue(sql.contains("LEFT JOIN"),
                "SQL should contain LEFT JOIN");

        // Check for table aliases
        Assertions.assertTrue(sql.contains("AS main"),
                "SQL should contain main table alias");
        Assertions.assertTrue(sql.contains("AS sub1"),
                "SQL should contain sub1 table alias");
        Assertions.assertTrue(sql.contains("AS sub2"),
                "SQL should contain sub2 table alias");
        Assertions.assertTrue(sql.contains("AS sub3"),
                "SQL should contain sub3 table alias");

        // Check for ON conditions
        Assertions.assertTrue(sql.contains("ON "),
                "SQL should contain ON conditions");

        // Check for WHERE clause
        Assertions.assertTrue(sql.contains("WHERE "),
                "SQL should contain WHERE clause");

        // Check for semicolon at the end
        Assertions.assertTrue(sql.trim().endsWith(";"),
                "SQL should end with semicolon");

        // Validate specific join conditions
        validateJoinConditions(sql);

        // Validate constants usage (only if there are literals)
        if (!target.getLiterals().isEmpty()) {
            validateConstantsUsage(sql, target);
        }
    }

    private void validateGeneratedSelectSql2(String sql, MvViewExpr target) {
        // Check for DECLARE statement
        Assertions.assertTrue(sql.startsWith("DECLARE $"),
                "SQL should start with DECLARE statement");
        Assertions.assertTrue(sql.contains("DECLARE " + MvSqlGen.SYS_KEYS_VAR + " AS "),
                "SQL should declare sys_keys parameter");

        // Check for List<Struct<...>> type declaration
        Assertions.assertTrue(sql.contains("List<Struct<"),
                "SQL should declare List<Struct<...>> type");

        // Check for primary key fields in type declaration
        Assertions.assertTrue(sql.contains("id:Int32"),
                "Type declaration should include id field");

        // Check for AS_TABLE subquery
        Assertions.assertTrue(sql.contains("AS_TABLE(" + MvSqlGen.SYS_KEYS_VAR + ") AS " + MvSqlGen.SYS_KEYS),
                "SQL should contain AS_TABLE subquery for sys_keys");

        // Check for INNER JOIN with main table
        Assertions.assertTrue(sql.contains("INNER JOIN"),
                "SQL should contain INNER JOIN with main table");

        // Check for ON condition with primary key
        Assertions.assertTrue(sql.contains("ON " + MvSqlGen.SYS_KEYS + ".id = main.id"),
                "SQL should contain ON condition for primary key");

        // Check for SELECT clause
        Assertions.assertTrue(sql.contains("SELECT"),
                "SQL should contain SELECT clause");

        // Check that all columns are present
        for (var column : target.getColumns()) {
            Assertions.assertTrue(sql.contains(" AS " + column.getName()),
                    "SQL should contain column: " + column.getName());
        }

        // Check for constants subquery (since we have literals in join conditions)
        // For SQL_GOOD2, there are no literals in join conditions, so this check is not applicable
        if (!target.getLiterals().isEmpty()) {
            Assertions.assertTrue(sql.contains("FROM (SELECT"),
                    "SQL should contain constants subquery");
            Assertions.assertTrue(sql.contains(") AS " + MvSqlGen.SYS_CONST),
                    "SQL should have constants alias");
        }

        // Check for CROSS JOIN with main table (only if there are literals)
        // For SQL_GOOD2, there are no literals, so no CROSS JOIN is needed
        if (!target.getLiterals().isEmpty()) {
            Assertions.assertTrue(sql.contains("CROSS JOIN"),
                    "SQL should contain CROSS JOIN for main table");
        } else {
            // Should have standard FROM/JOIN structure
            Assertions.assertTrue(sql.contains("FROM "),
                    "SQL should contain FROM clause");
        }

        // Check for other joins
        Assertions.assertTrue(sql.contains("INNER JOIN"),
                "SQL should contain INNER JOIN");
        Assertions.assertTrue(sql.contains("LEFT JOIN"),
                "SQL should contain LEFT JOIN");

        // Check for table aliases
        Assertions.assertTrue(sql.contains("AS main"),
                "SQL should contain main table alias");
        Assertions.assertTrue(sql.contains("AS sub1"),
                "SQL should contain sub1 table alias");
        Assertions.assertTrue(sql.contains("AS sub2"),
                "SQL should contain sub2 table alias");
        Assertions.assertTrue(sql.contains("AS sub3"),
                "SQL should contain sub3 table alias");

        // Check for ON conditions
        Assertions.assertTrue(sql.contains("ON "),
                "SQL should contain ON conditions");

        // Check for WHERE clause
        Assertions.assertTrue(sql.contains("WHERE "),
                "SQL should contain WHERE clause");

        // Check for semicolon at the end
        Assertions.assertTrue(sql.trim().endsWith(";"),
                "SQL should end with semicolon");

        // Validate specific join conditions for SQL_GOOD2
        validateJoinConditions2(sql);
    }

    private void validateJoinConditions(String sql) {
        // Check that join conditions reference constants properly
        Assertions.assertTrue(sql.contains(MvSqlGen.SYS_CONST + ".c0"),
                "SQL should reference constant c0");
        Assertions.assertTrue(sql.contains(MvSqlGen.SYS_CONST + ".c1"),
                "SQL should reference constant c1");

        // Check that regular column references are present
        Assertions.assertTrue(sql.contains("main.c1"),
                "SQL should contain main.c1 reference");
        Assertions.assertTrue(sql.contains("sub1.c1"),
                "SQL should contain sub1.c1 reference");
        Assertions.assertTrue(sql.contains("main.c2"),
                "SQL should contain main.c2 reference");
        Assertions.assertTrue(sql.contains("sub1.c2"),
                "SQL should contain sub1.c2 reference");
        Assertions.assertTrue(sql.contains("main.c3"),
                "SQL should contain main.c3 reference");
        Assertions.assertTrue(sql.contains("sub2.c3"),
                "SQL should contain sub2.c3 reference");
        Assertions.assertTrue(sql.contains("sub2.c4"),
                "SQL should contain sub2.c4 reference");
        Assertions.assertTrue(sql.contains("sub3.c5"),
                "SQL should contain sub3.c5 reference");
    }

    private void validateJoinConditions2(String sql) {
        // For SQL_GOOD2, there are no literal constants in join conditions
        // All joins are column-to-column comparisons

        // Check that regular column references are present
        Assertions.assertTrue(sql.contains("main.c1"),
                "SQL should contain main.c1 reference");
        Assertions.assertTrue(sql.contains("sub1.c1"),
                "SQL should contain sub1.c1 reference");
        Assertions.assertTrue(sql.contains("main.c2"),
                "SQL should contain main.c2 reference");
        Assertions.assertTrue(sql.contains("sub1.c2"),
                "SQL should contain sub1.c2 reference");
        Assertions.assertTrue(sql.contains("main.c3"),
                "SQL should contain main.c3 reference");
        Assertions.assertTrue(sql.contains("sub2.c3"),
                "SQL should contain sub2.c3 reference");
        Assertions.assertTrue(sql.contains("main.c4"),
                "SQL should contain main.c4 reference");
        Assertions.assertTrue(sql.contains("sub2.c4"),
                "SQL should contain sub2.c4 reference");
        Assertions.assertTrue(sql.contains("sub2.c5"),
                "SQL should contain sub2.c5 reference");
        Assertions.assertTrue(sql.contains("sub3.c5"),
                "SQL should contain sub3.c5 reference");
    }

    private void validateConstantsUsage(String sql, MvViewExpr target) {
        // Check that all literals are properly formatted in the constants subquery
        for (var literal : target.getLiterals()) {
            String value = literal.getValue();
            // String literals should be quoted, integer literals should not
            if (value.startsWith("'") && value.endsWith("'")) {
                // String literal - should appear as quoted in SQL
                Assertions.assertTrue(sql.contains(value),
                        "SQL should contain string literal: " + value);
            } else {
                // Integer or other literal - should appear as-is in SQL
                Assertions.assertTrue(sql.contains(value),
                        "SQL should contain literal as-is: " + value);
            }
        }

        // Check that constants are referenced with proper identity
        for (var literal : target.getLiterals()) {
            Assertions.assertTrue(sql.contains(MvSqlGen.SYS_CONST + "." + literal.getIdentity()),
                    "SQL should reference constant with identity: " + literal.getIdentity());
        }
    }
}
