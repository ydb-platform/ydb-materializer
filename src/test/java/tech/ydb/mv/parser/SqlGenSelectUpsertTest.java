package tech.ydb.mv.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.SqlConstants;
import tech.ydb.mv.model.MvMetadata;

/**
 * Test for MvSqlGen.makeSelect() and MvSqlGen.makeUpsert() methods
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

    private void addTableInfoToTarget(tech.ydb.mv.model.MvViewExpr target) {
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

    private void addTableInfoToTarget2(tech.ydb.mv.model.MvViewExpr target) {
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

    private void validateGeneratedSelectSql1(String sql, tech.ydb.mv.model.MvViewExpr target) {
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

    private void validateGeneratedSelectSql2(String sql, tech.ydb.mv.model.MvViewExpr target) {
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

    private void validateConstantsUsage(String sql, tech.ydb.mv.model.MvViewExpr target) {
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
