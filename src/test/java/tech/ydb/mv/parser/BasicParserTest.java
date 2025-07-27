package tech.ydb.mv.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvTableRef;

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
       sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10
FROM main_table AS main /* something */
INNER JOIN sub_table1 AS sub1
  ON main.c1=sub1.c1 AND main.c2=sub1.c2
LEFT JOIN sub_table2 AS sub2
  ON main.c3=sub2.c3 AND 'val1'=sub2.c4
INNER JOIN sub_table3 AS sub3
  ON sub3.c5=58
WHERE COMPUTE ON main, sub2
#[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2') ]#;

PROCESS main_table CHANGEFEED cf1 AS STREAM;
PROCESS sub_table1 CHANGEFEED cf1 AS STREAM;
PROCESS sub_table2 CHANGEFEED cf1 AS STREAM;
PROCESS sub_table3 CHANGEFEED cf1 AS BATCH;
""";

    @Test
    public void parserTest() {
        MvContext mc = new MvParser(SQL1).fill();

        // Test MvContext structure
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(0, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());
        Assertions.assertEquals(1, mc.getViews().size());
        Assertions.assertEquals(4, mc.getInputs().size());

        // Test MvTarget (view) structure
        var view0 = mc.getViews().get(0);
        Assertions.assertEquals("m1", view0.getName());
        Assertions.assertEquals(4, view0.getSources().size());
        Assertions.assertEquals(7, view0.getColumns().size());
        Assertions.assertNotNull(view0.getFilter());

        // Test MvTableRef sources
        var mainSource = view0.getSources().get(0);
        Assertions.assertEquals("main_table", mainSource.getReference());
        Assertions.assertEquals("main", mainSource.getAlias());
        Assertions.assertEquals(MvTableRef.Mode.MAIN, mainSource.getMode());
        Assertions.assertEquals(0, mainSource.getConditions().size());

        var sub1Source = view0.getSources().get(1);
        Assertions.assertEquals("sub_table1", sub1Source.getReference());
        Assertions.assertEquals("sub1", sub1Source.getAlias());
        Assertions.assertEquals(MvTableRef.Mode.INNER, sub1Source.getMode());
        Assertions.assertEquals(2, sub1Source.getConditions().size());

        var sub2Source = view0.getSources().get(2);
        Assertions.assertEquals("sub_table2", sub2Source.getReference());
        Assertions.assertEquals("sub2", sub2Source.getAlias());
        Assertions.assertEquals(MvTableRef.Mode.LEFT, sub2Source.getMode());
        Assertions.assertEquals(2, sub2Source.getConditions().size());

        var sub3Source = view0.getSources().get(3);
        Assertions.assertEquals("sub_table3", sub3Source.getReference());
        Assertions.assertEquals("sub3", sub3Source.getAlias());
        Assertions.assertEquals(MvTableRef.Mode.INNER, sub3Source.getMode());
        Assertions.assertEquals(1, sub3Source.getConditions().size());

        // Test MvJoinCondition for sub1
        var sub1Cond1 = sub1Source.getConditions().get(0);
        Assertions.assertEquals("main", sub1Cond1.getFirstAlias());
        Assertions.assertEquals("c1", sub1Cond1.getFirstColumn());
        Assertions.assertEquals("sub1", sub1Cond1.getSecondAlias());
        Assertions.assertEquals("c1", sub1Cond1.getSecondColumn());
        Assertions.assertNull(sub1Cond1.getFirstLiteral());
        Assertions.assertNull(sub1Cond1.getSecondLiteral());

        var sub1Cond2 = sub1Source.getConditions().get(1);
        Assertions.assertEquals("main", sub1Cond2.getFirstAlias());
        Assertions.assertEquals("c2", sub1Cond2.getFirstColumn());
        Assertions.assertEquals("sub1", sub1Cond2.getSecondAlias());
        Assertions.assertEquals("c2", sub1Cond2.getSecondColumn());
        Assertions.assertNull(sub1Cond2.getFirstLiteral());
        Assertions.assertNull(sub1Cond2.getSecondLiteral());

        // Test MvJoinCondition for sub2
        var sub2Cond1 = sub2Source.getConditions().get(0);
        Assertions.assertEquals("main", sub2Cond1.getFirstAlias());
        Assertions.assertEquals("c3", sub2Cond1.getFirstColumn());
        Assertions.assertEquals("sub2", sub2Cond1.getSecondAlias());
        Assertions.assertEquals("c3", sub2Cond1.getSecondColumn());
        Assertions.assertNull(sub2Cond1.getFirstLiteral());
        Assertions.assertNull(sub2Cond1.getSecondLiteral());

        var sub2Cond2 = sub2Source.getConditions().get(1);
        Assertions.assertNull(sub2Cond2.getFirstAlias());
        Assertions.assertNull(sub2Cond2.getFirstColumn());
        Assertions.assertEquals("sub2", sub2Cond2.getSecondAlias());
        Assertions.assertEquals("c4", sub2Cond2.getSecondColumn());
        Assertions.assertEquals("'val1'", sub2Cond2.getFirstLiteral());
        Assertions.assertNull(sub2Cond2.getSecondLiteral());

        // Test MvJoinCondition for sub3
        var sub3Cond1 = sub3Source.getConditions().get(0);
        Assertions.assertEquals("sub3", sub3Cond1.getFirstAlias());
        Assertions.assertEquals("c5", sub3Cond1.getFirstColumn());
        Assertions.assertNull(sub3Cond1.getFirstLiteral());
        Assertions.assertNull(sub3Cond1.getSecondAlias());
        Assertions.assertNull(sub3Cond1.getSecondColumn());
        Assertions.assertEquals("58", sub3Cond1.getSecondLiteral());

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

        // Test MvComputation filter
        Assertions.assertEquals(" main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2') ", view0.getFilter().getExpression());
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
        Assertions.assertEquals("sub_table3", input4.getTableName());
        Assertions.assertEquals("cf1", input4.getChangeFeed());
        Assertions.assertTrue(input4.isBatchMode());
    }

}
