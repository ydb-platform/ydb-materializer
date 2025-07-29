package tech.ydb.mv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.parser.MvParser;

/**
 * Test for SqlGen.makeCreateView() method
 *
 * @author zinal
 */
public class SqlGenTest {

    @Test
    public void testMakeCreateView() {
        // Parse the input SQL
        MvContext mc = new MvParser(SqlConstants.SQL1).fill();

        // Verify parsing was successful
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(1, mc.getTargets().size());

        // Get the target and generate SQL
        var target = mc.getTargets().get(0);
        SqlGen sqlGen = new SqlGen(target);
        String generatedSql = sqlGen.makeCreateView();

        // Print the generated SQL for debugging
        System.out.println("Generated SQL:");
        System.out.println(generatedSql);

        // Validate the generated SQL structure
        validateGeneratedSql(generatedSql, target);
    }

    private void validateGeneratedSql(String sql, tech.ydb.mv.model.MvTarget target) {
        // Check that SQL starts with CREATE VIEW
        Assertions.assertTrue(sql.startsWith("CREATE VIEW "),
            "SQL should start with 'CREATE VIEW'");

        // Check that view name is properly quoted
        Assertions.assertTrue(sql.contains("CREATE VIEW `m1`"),
            "View name should be properly quoted");

        // Check for WITH clause
        Assertions.assertTrue(sql.contains("WITH (security_invoker=TRUE) AS"),
            "SQL should contain WITH clause");

        // Check for SELECT clause
        Assertions.assertTrue(sql.contains("SELECT"),
            "SQL should contain SELECT clause");

        // Check that all columns are present
        for (var column : target.getColumns()) {
            Assertions.assertTrue(sql.contains("`" + column.getName() + "`"),
                "SQL should contain column: " + column.getName());
        }

        // Check for constants subquery (since we have literals in join conditions)
        Assertions.assertTrue(target.getLiterals().size() > 0,
            "Target should have literals");
        Assertions.assertTrue(sql.contains("FROM (SELECT"),
            "SQL should contain constants subquery");
        Assertions.assertTrue(sql.contains(") AS constants"),
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

        // Validate constants usage
        validateConstantsUsage(sql, target);
    }

    private void validateJoinConditions(String sql) {
        // Check that join conditions reference constants properly
        Assertions.assertTrue(sql.contains("constants.`c0`"),
            "SQL should reference constant c0");
        Assertions.assertTrue(sql.contains("constants.`c1`"),
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

    private void validateConstantsUsage(String sql, tech.ydb.mv.model.MvTarget target) {
        // Check that all literals are properly quoted in the constants subquery
        for (var literal : target.getLiterals()) {
            // The literal value is stored with quotes, so we need to extract the actual value
            String value = literal.getValue();
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            String expectedQuotedValue = "'" + value + "'";
            Assertions.assertTrue(sql.contains(expectedQuotedValue),
                "SQL should contain properly quoted literal: " + expectedQuotedValue);
        }

        // Check that constants are referenced with proper identity
        for (var literal : target.getLiterals()) {
            Assertions.assertTrue(sql.contains("constants.`" + literal.getIdentity() + "`"),
                "SQL should reference constant with identity: " + literal.getIdentity());
        }
    }
}