package tech.ydb.mv.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinSource;

/**
 *
 * @author zinal
 */
public class BasicParserTest {

    private static final String SQL1 =
"""
-- async view demo (with something strange within)
CREATE ASYNC MATERIALIZED VIEW m1 AS
SELECT main.id AS id, main.c1 AS c1, main . c2 AS c2, main . c3 AS c3,
       sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10,
       COMPUTE ON main #[ Substring(main.c20,3,5) ]# AS c11,
       COMPUTE #[ CAST(NULL AS Int32?) ]# AS c12
FROM main_table AS main /* something */
INNER JOIN sub_table1 AS sub1
  ON main.c1=sub1.c1 AND main.c2=sub1.c2
LEFT JOIN sub_table2 AS sub2
  ON main.c3=sub2.c3 AND 'val1'=sub2.c4
INNER JOIN `schema2/sub_table3` AS sub3
  ON sub3.c5=58
WHERE COMPUTE ON main, sub2
#[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2') ]#;

PROCESS main_table CHANGEFEED cf1 AS STREAM;
PROCESS sub_table1 CHANGEFEED cf1 AS STREAM;
PROCESS sub_table2 CHANGEFEED cf1 AS STREAM;
PROCESS `schema2/sub_table3` CHANGEFEED cf1 AS BATCH;
""";

    @Test
    public void parserTest() {
        MvContext mc = new MvParser(SQL1).fill();

        // Test MvContext structure
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(0, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());
        Assertions.assertEquals(1, mc.getTargets().size());
        Assertions.assertEquals(4, mc.getInputs().size());

        // Test MvTarget (view) structure
        var view0 = mc.getTargets().get(0);
        Assertions.assertEquals("m1", view0.getName());
        Assertions.assertEquals(4, view0.getSources().size());
        Assertions.assertEquals(9, view0.getColumns().size());
        Assertions.assertNotNull(view0.getFilter());

        // Test MvTableRef sources
        var mainSource = view0.getSources().get(0);
        Assertions.assertEquals("main_table", mainSource.getTableName());
        Assertions.assertEquals("main", mainSource.getAlias());
        Assertions.assertEquals(MvJoinSource.Mode.MAIN, mainSource.getMode());
        Assertions.assertEquals(0, mainSource.getConditions().size());

        var sub1Source = view0.getSources().get(1);
        Assertions.assertEquals("sub_table1", sub1Source.getTableName());
        Assertions.assertEquals("sub1", sub1Source.getAlias());
        Assertions.assertEquals(MvJoinSource.Mode.INNER, sub1Source.getMode());
        Assertions.assertEquals(2, sub1Source.getConditions().size());

        var sub2Source = view0.getSources().get(2);
        Assertions.assertEquals("sub_table2", sub2Source.getTableName());
        Assertions.assertEquals("sub2", sub2Source.getAlias());
        Assertions.assertEquals(MvJoinSource.Mode.LEFT, sub2Source.getMode());
        Assertions.assertEquals(2, sub2Source.getConditions().size());

        var sub3Source = view0.getSources().get(3);
        Assertions.assertEquals("`schema2/sub_table3`", sub3Source.getTableName());
        Assertions.assertEquals("sub3", sub3Source.getAlias());
        Assertions.assertEquals(MvJoinSource.Mode.INNER, sub3Source.getMode());
        Assertions.assertEquals(1, sub3Source.getConditions().size());

        // Test MvJoinCondition for sub1
        var sub1Cond1 = sub1Source.getConditions().get(0);
        checkJoinCondition(sub1Cond1, "main", "c1", null, "sub1", "c1", null);
        var sub1Cond2 = sub1Source.getConditions().get(1);
        checkJoinCondition(sub1Cond2, "main", "c2", null, "sub1", "c2", null);

        // Test MvJoinCondition for sub2
        var sub2Cond1 = sub2Source.getConditions().get(0);
        checkJoinCondition(sub2Cond1, "main", "c3", null, "sub2", "c3", null);
        var sub2Cond2 = sub2Source.getConditions().get(1);
        checkJoinCondition(sub2Cond2, null, null, "'val1'", "sub2", "c4", null);

        // Test MvJoinCondition for sub3
        var sub3Cond1 = sub3Source.getConditions().get(0);
        checkJoinCondition(sub3Cond1, "sub3", "c5", null, null, null, "58");

        // Test MvColumn structure
        Assertions.assertEquals("id", view0.getColumns().get(0).getName());
        Assertions.assertEquals("main", view0.getColumns().get(0).getSourceAlias());
        Assertions.assertEquals("id", view0.getColumns().get(0).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(0).isComputation());
        Assertions.assertNull(view0.getColumns().get(0).getComputation());

        Assertions.assertEquals("c1", view0.getColumns().get(1).getName());
        Assertions.assertEquals("main", view0.getColumns().get(1).getSourceAlias());
        Assertions.assertEquals("c1", view0.getColumns().get(1).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(1).isComputation());
        Assertions.assertNull(view0.getColumns().get(1).getComputation());

        Assertions.assertEquals("c2", view0.getColumns().get(2).getName());
        Assertions.assertEquals("main", view0.getColumns().get(2).getSourceAlias());
        Assertions.assertEquals("c2", view0.getColumns().get(2).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(2).isComputation());
        Assertions.assertNull(view0.getColumns().get(2).getComputation());

        Assertions.assertEquals("c3", view0.getColumns().get(3).getName());
        Assertions.assertEquals("main", view0.getColumns().get(3).getSourceAlias());
        Assertions.assertEquals("c3", view0.getColumns().get(3).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(3).isComputation());
        Assertions.assertNull(view0.getColumns().get(3).getComputation());

        Assertions.assertEquals("c8", view0.getColumns().get(4).getName());
        Assertions.assertEquals("sub1", view0.getColumns().get(4).getSourceAlias());
        Assertions.assertEquals("c8", view0.getColumns().get(4).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(4).isComputation());
        Assertions.assertNull(view0.getColumns().get(4).getComputation());

        Assertions.assertEquals("c9", view0.getColumns().get(5).getName());
        Assertions.assertEquals("sub2", view0.getColumns().get(5).getSourceAlias());
        Assertions.assertEquals("c9", view0.getColumns().get(5).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(5).isComputation());
        Assertions.assertNull(view0.getColumns().get(5).getComputation());

        Assertions.assertEquals("c10", view0.getColumns().get(6).getName());
        Assertions.assertEquals("sub3", view0.getColumns().get(6).getSourceAlias());
        Assertions.assertEquals("c10", view0.getColumns().get(6).getSourceColumn());
        Assertions.assertFalse(view0.getColumns().get(6).isComputation());
        Assertions.assertNull(view0.getColumns().get(6).getComputation());

        Assertions.assertEquals("c11", view0.getColumns().get(7).getName());
        Assertions.assertNull(view0.getColumns().get(7).getSourceAlias());
        Assertions.assertNull(view0.getColumns().get(7).getSourceColumn());
        Assertions.assertTrue(view0.getColumns().get(7).isComputation());
        Assertions.assertEquals("Substring(main.c20,3,5)",
                view0.getColumns().get(7).getComputation().getExpression());
        Assertions.assertEquals(1,
                view0.getColumns().get(7).getComputation().getSources().size());
        Assertions.assertEquals("main",
                view0.getColumns().get(7).getComputation().getSources().get(0).getAlias());
        Assertions.assertEquals("main",
                view0.getColumns().get(7).getComputation().getSources().get(0).getReference().getAlias());

        Assertions.assertEquals("c12", view0.getColumns().get(8).getName());
        Assertions.assertNull(view0.getColumns().get(8).getSourceAlias());
        Assertions.assertNull(view0.getColumns().get(8).getSourceColumn());
        Assertions.assertTrue(view0.getColumns().get(8).isComputation());
        Assertions.assertEquals("CAST(NULL AS Int32?)",
                view0.getColumns().get(8).getComputation().getExpression());
        Assertions.assertEquals(0,
                view0.getColumns().get(8).getComputation().getSources().size());

        // Test MvComputation filter
        Assertions.assertEquals("main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2')", view0.getFilter().getExpression());
        Assertions.assertEquals(2, view0.getFilter().getSources().size());
        Assertions.assertEquals("main", view0.getFilter().getSources().get(0).getAlias());
        Assertions.assertEquals("sub2", view0.getFilter().getSources().get(1).getAlias());

        // Test MvInput structure
        var input1 = mc.getInputs().get(0);
        Assertions.assertEquals("main_table", input1.getTableName());
        Assertions.assertEquals("cf1", input1.getChangeFeed());
        Assertions.assertFalse(input1.isBatchMode());

        var input2 = mc.getInputs().get(1);
        Assertions.assertEquals("sub_table1", input2.getTableName());
        Assertions.assertEquals("cf1", input2.getChangeFeed());
        Assertions.assertFalse(input2.isBatchMode());

        var input3 = mc.getInputs().get(2);
        Assertions.assertEquals("sub_table2", input3.getTableName());
        Assertions.assertEquals("cf1", input3.getChangeFeed());
        Assertions.assertFalse(input3.isBatchMode());

        var input4 = mc.getInputs().get(3);
        Assertions.assertEquals("`schema2/sub_table3`", input4.getTableName());
        Assertions.assertEquals("cf1", input4.getChangeFeed());
        Assertions.assertTrue(input4.isBatchMode());
    }

    private void checkJoinCondition(MvJoinCondition cond,
            String firstAlias, String firstColumn, String firstLiteral,
            String secondAlias, String secondColumn, String secondLiteral) {
        if (firstLiteral==null) {
            Assertions.assertNull(cond.getFirstLiteral());
            Assertions.assertEquals(firstAlias, cond.getFirstAlias());
            Assertions.assertEquals(firstColumn, cond.getFirstColumn());
            Assertions.assertNotNull(cond.getFirstRef());
            Assertions.assertEquals(firstAlias, cond.getFirstRef().getAlias());
        } else {
            Assertions.assertEquals(firstLiteral, cond.getFirstLiteral());
            Assertions.assertNull(cond.getFirstAlias());
            Assertions.assertNull(cond.getFirstColumn());
            Assertions.assertNull(cond.getFirstRef());
        }
        if (secondLiteral==null) {
            Assertions.assertNull(cond.getSecondLiteral());
            Assertions.assertEquals(secondAlias, cond.getSecondAlias());
            Assertions.assertEquals(secondColumn, cond.getSecondColumn());
            Assertions.assertNotNull(cond.getSecondRef());
            Assertions.assertEquals(secondAlias, cond.getSecondRef().getAlias());
        } else {
            Assertions.assertEquals(secondLiteral, cond.getSecondLiteral());
            Assertions.assertNull(cond.getSecondAlias());
            Assertions.assertNull(cond.getSecondColumn());
            Assertions.assertNull(cond.getSecondRef());
        }
    }

}
