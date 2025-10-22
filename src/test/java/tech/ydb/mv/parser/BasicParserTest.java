package tech.ydb.mv.parser;

import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.SqlConstants;
import tech.ydb.mv.support.MvIssuePrinter;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public class BasicParserTest {

    private static final boolean PRINT_SQL = SqlConstants.PRINT_SQL;

    @Test
    public void parserTest1() {
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_GOOD1).fill();
        if (PRINT_SQL) {
            new MvIssuePrinter(mc).write(System.out);
        }

        // Test MvContext structure
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(0, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());
        Assertions.assertEquals(1, mc.getViews().size());
        Assertions.assertEquals(1, mc.getHandlers().size());
        Assertions.assertEquals(4, mc.getHandlers().values().iterator().next().getInputs().size());

        // Test MvTarget (view) structure
        var view0 = mc.getViews().values().iterator().next();
        var target0 = view0.getTargets().values().iterator().next();
        Assertions.assertEquals("m1", view0.getViewName());
        Assertions.assertEquals("m1", target0.getViewName());
        Assertions.assertEquals(4, target0.getSources().size());
        Assertions.assertEquals(9, target0.getColumns().size());
        Assertions.assertNotNull(target0.getFilter());
        Assertions.assertNotNull(target0.getLiterals());
        Assertions.assertEquals(2, target0.getLiterals().size());

        // Test MvTableRef sources
        var mainSource = target0.getSources().get(0);
        Assertions.assertEquals("main_table", mainSource.getTableName());
        Assertions.assertEquals("main", mainSource.getTableAlias());
        Assertions.assertEquals(MvJoinMode.MAIN, mainSource.getMode());
        Assertions.assertEquals(0, mainSource.getConditions().size());

        var sub1Source = target0.getSources().get(1);
        Assertions.assertEquals("sub_table1", sub1Source.getTableName());
        Assertions.assertEquals("sub1", sub1Source.getTableAlias());
        Assertions.assertEquals(MvJoinMode.INNER, sub1Source.getMode());
        Assertions.assertEquals(2, sub1Source.getConditions().size());

        var sub2Source = target0.getSources().get(2);
        Assertions.assertEquals("sub_table2", sub2Source.getTableName());
        Assertions.assertEquals("sub2", sub2Source.getTableAlias());
        Assertions.assertEquals(MvJoinMode.LEFT, sub2Source.getMode());
        Assertions.assertEquals(2, sub2Source.getConditions().size());

        var sub3Source = target0.getSources().get(3);
        Assertions.assertEquals("sub_table3", sub3Source.getTableName());
        Assertions.assertEquals("sub3", sub3Source.getTableAlias());
        Assertions.assertEquals(MvJoinMode.INNER, sub3Source.getMode());
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
        Assertions.assertEquals("id", target0.getColumns().get(0).getName());
        Assertions.assertEquals("main", target0.getColumns().get(0).getSourceAlias());
        Assertions.assertEquals("id", target0.getColumns().get(0).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(0).isComputation());
        Assertions.assertNull(target0.getColumns().get(0).getComputation());

        Assertions.assertEquals("c1", target0.getColumns().get(1).getName());
        Assertions.assertEquals("main", target0.getColumns().get(1).getSourceAlias());
        Assertions.assertEquals("c1", target0.getColumns().get(1).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(1).isComputation());
        Assertions.assertNull(target0.getColumns().get(1).getComputation());

        Assertions.assertEquals("c2", target0.getColumns().get(2).getName());
        Assertions.assertEquals("main", target0.getColumns().get(2).getSourceAlias());
        Assertions.assertEquals("c2", target0.getColumns().get(2).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(2).isComputation());
        Assertions.assertNull(target0.getColumns().get(2).getComputation());

        Assertions.assertEquals("c3", target0.getColumns().get(3).getName());
        Assertions.assertEquals("main", target0.getColumns().get(3).getSourceAlias());
        Assertions.assertEquals("c3", target0.getColumns().get(3).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(3).isComputation());
        Assertions.assertNull(target0.getColumns().get(3).getComputation());

        Assertions.assertEquals("c8", target0.getColumns().get(4).getName());
        Assertions.assertEquals("sub1", target0.getColumns().get(4).getSourceAlias());
        Assertions.assertEquals("c8", target0.getColumns().get(4).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(4).isComputation());
        Assertions.assertNull(target0.getColumns().get(4).getComputation());

        Assertions.assertEquals("c9", target0.getColumns().get(5).getName());
        Assertions.assertEquals("sub2", target0.getColumns().get(5).getSourceAlias());
        Assertions.assertEquals("c9", target0.getColumns().get(5).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(5).isComputation());
        Assertions.assertNull(target0.getColumns().get(5).getComputation());

        Assertions.assertEquals("c10", target0.getColumns().get(6).getName());
        Assertions.assertEquals("sub3", target0.getColumns().get(6).getSourceAlias());
        Assertions.assertEquals("c10", target0.getColumns().get(6).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(6).isComputation());
        Assertions.assertNull(target0.getColumns().get(6).getComputation());

        Assertions.assertEquals("c11", target0.getColumns().get(7).getName());
        Assertions.assertNull(target0.getColumns().get(7).getSourceAlias());
        Assertions.assertNull(target0.getColumns().get(7).getSourceColumn());
        Assertions.assertTrue(target0.getColumns().get(7).isComputation());
        Assertions.assertEquals("Substring(main.c20,3,5)",
                target0.getColumns().get(7).getComputation().getExpression());
        Assertions.assertEquals(1,
                target0.getColumns().get(7).getComputation().getSources().size());
        Assertions.assertEquals("main",
                target0.getColumns().get(7).getComputation().getSources().get(0).getAlias());
        Assertions.assertEquals("main",
                target0.getColumns().get(7).getComputation().getSources().get(0).getReference().getTableAlias());

        Assertions.assertEquals("c12", target0.getColumns().get(8).getName());
        Assertions.assertNull(target0.getColumns().get(8).getSourceAlias());
        Assertions.assertNull(target0.getColumns().get(8).getSourceColumn());
        Assertions.assertTrue(target0.getColumns().get(8).isComputation());
        Assertions.assertEquals("CAST(NULL AS Int32?)",
                target0.getColumns().get(8).getComputation().getExpression());
        Assertions.assertEquals(0,
                target0.getColumns().get(8).getComputation().getSources().size());

        // Test MvComputation filter
        Assertions.assertEquals("main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2')", target0.getFilter().getExpression());
        Assertions.assertEquals(2, target0.getFilter().getSources().size());
        Assertions.assertEquals("main", target0.getFilter().getSources().get(0).getAlias());
        Assertions.assertEquals("sub2", target0.getFilter().getSources().get(1).getAlias());

        // Test MvInput structure
        var handler1 = mc.getHandlers().values().iterator().next();
        var input1 = handler1.getInputs().get("main_table");
        Assertions.assertEquals("main_table", input1.getTableName());
        Assertions.assertEquals("cf1", input1.getChangefeed());
        Assertions.assertFalse(input1.isBatchMode());

        var input2 = handler1.getInputs().get("sub_table1");
        Assertions.assertEquals("sub_table1", input2.getTableName());
        Assertions.assertEquals("cf1", input2.getChangefeed());
        Assertions.assertFalse(input2.isBatchMode());

        var input3 = handler1.getInputs().get("sub_table2");
        Assertions.assertEquals("sub_table2", input3.getTableName());
        Assertions.assertEquals("cf1", input3.getChangefeed());
        Assertions.assertFalse(input3.isBatchMode());

        var input4 = handler1.getInputs().get("sub_table3");
        Assertions.assertEquals("sub_table3", input4.getTableName());
        Assertions.assertEquals("cf1", input4.getChangefeed());
        Assertions.assertTrue(input4.isBatchMode());
    }

    @Test
    public void parserTest2() {
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_GOOD2).fill();
        if (PRINT_SQL) {
            new MvIssuePrinter(mc).write(System.out);
        }

        // Test MvContext structure
        Assertions.assertTrue(mc.isValid());
        Assertions.assertEquals(0, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());
        Assertions.assertEquals(1, mc.getViews().size());
        Assertions.assertEquals(1, mc.getHandlers().size());
        Assertions.assertEquals(4, mc.getHandlers().values().iterator().next().getInputs().size());

        // Test MvTarget (view) structure
        var view0 = mc.getViews().values().iterator().next();
        var target0 = view0.getTargets().values().iterator().next();
        Assertions.assertEquals("schema3/mv1", target0.getViewName());
        Assertions.assertEquals(4, target0.getSources().size());
        Assertions.assertEquals(9, target0.getColumns().size());
        Assertions.assertNotNull(target0.getFilter());
        Assertions.assertNotNull(target0.getLiterals());
        Assertions.assertEquals(0, target0.getLiterals().size());

        // Test MvTableRef sources
        var mainSource = target0.getSources().get(0);
        Assertions.assertEquals("schema3/main_table", mainSource.getTableName());
        Assertions.assertEquals("main", mainSource.getTableAlias());
        Assertions.assertEquals(MvJoinMode.MAIN, mainSource.getMode());
        Assertions.assertEquals(0, mainSource.getConditions().size());

        var sub1Source = target0.getSources().get(1);
        Assertions.assertEquals("schema3/sub_table1", sub1Source.getTableName());
        Assertions.assertEquals("sub1", sub1Source.getTableAlias());
        Assertions.assertEquals(MvJoinMode.INNER, sub1Source.getMode());
        Assertions.assertEquals(2, sub1Source.getConditions().size());

        var sub2Source = target0.getSources().get(2);
        Assertions.assertEquals("schema3/sub_table2", sub2Source.getTableName());
        Assertions.assertEquals("sub2", sub2Source.getTableAlias());
        Assertions.assertEquals(MvJoinMode.LEFT, sub2Source.getMode());
        Assertions.assertEquals(2, sub2Source.getConditions().size());

        var sub3Source = target0.getSources().get(3);
        Assertions.assertEquals("schema3/sub_table3", sub3Source.getTableName());
        Assertions.assertEquals("sub3", sub3Source.getTableAlias());
        Assertions.assertEquals(MvJoinMode.INNER, sub3Source.getMode());
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
        checkJoinCondition(sub2Cond2, "main", "c4", null, "sub2", "c4", null);

        // Test MvJoinCondition for sub3
        var sub3Cond1 = sub3Source.getConditions().get(0);
        checkJoinCondition(sub3Cond1, "sub3", "c5", null, "sub2", "c5", null);

        // Test MvColumn structure
        Assertions.assertEquals("id", target0.getColumns().get(0).getName());
        Assertions.assertEquals("main", target0.getColumns().get(0).getSourceAlias());
        Assertions.assertEquals("id", target0.getColumns().get(0).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(0).isComputation());
        Assertions.assertNull(target0.getColumns().get(0).getComputation());

        Assertions.assertEquals("c1", target0.getColumns().get(1).getName());
        Assertions.assertEquals("main", target0.getColumns().get(1).getSourceAlias());
        Assertions.assertEquals("c1", target0.getColumns().get(1).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(1).isComputation());
        Assertions.assertNull(target0.getColumns().get(1).getComputation());

        Assertions.assertEquals("c2", target0.getColumns().get(2).getName());
        Assertions.assertEquals("main", target0.getColumns().get(2).getSourceAlias());
        Assertions.assertEquals("c2", target0.getColumns().get(2).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(2).isComputation());
        Assertions.assertNull(target0.getColumns().get(2).getComputation());

        Assertions.assertEquals("c3", target0.getColumns().get(3).getName());
        Assertions.assertEquals("main", target0.getColumns().get(3).getSourceAlias());
        Assertions.assertEquals("c3", target0.getColumns().get(3).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(3).isComputation());
        Assertions.assertNull(target0.getColumns().get(3).getComputation());

        Assertions.assertEquals("c8", target0.getColumns().get(4).getName());
        Assertions.assertEquals("sub1", target0.getColumns().get(4).getSourceAlias());
        Assertions.assertEquals("c8", target0.getColumns().get(4).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(4).isComputation());
        Assertions.assertNull(target0.getColumns().get(4).getComputation());

        Assertions.assertEquals("c9", target0.getColumns().get(5).getName());
        Assertions.assertEquals("sub2", target0.getColumns().get(5).getSourceAlias());
        Assertions.assertEquals("c9", target0.getColumns().get(5).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(5).isComputation());
        Assertions.assertNull(target0.getColumns().get(5).getComputation());

        Assertions.assertEquals("c10", target0.getColumns().get(6).getName());
        Assertions.assertEquals("sub3", target0.getColumns().get(6).getSourceAlias());
        Assertions.assertEquals("c10", target0.getColumns().get(6).getSourceColumn());
        Assertions.assertFalse(target0.getColumns().get(6).isComputation());
        Assertions.assertNull(target0.getColumns().get(6).getComputation());

        Assertions.assertEquals("c11", target0.getColumns().get(7).getName());
        Assertions.assertNull(target0.getColumns().get(7).getSourceAlias());
        Assertions.assertNull(target0.getColumns().get(7).getSourceColumn());
        Assertions.assertTrue(target0.getColumns().get(7).isComputation());
        Assertions.assertEquals("Substring(main.c20,3,5)",
                target0.getColumns().get(7).getComputation().getExpression());
        Assertions.assertEquals(1,
                target0.getColumns().get(7).getComputation().getSources().size());
        Assertions.assertEquals("main",
                target0.getColumns().get(7).getComputation().getSources().get(0).getAlias());
        Assertions.assertEquals("main",
                target0.getColumns().get(7).getComputation().getSources().get(0).getReference().getTableAlias());

        Assertions.assertEquals("c12", target0.getColumns().get(8).getName());
        Assertions.assertNull(target0.getColumns().get(8).getSourceAlias());
        Assertions.assertNull(target0.getColumns().get(8).getSourceColumn());
        Assertions.assertTrue(target0.getColumns().get(8).isComputation());
        Assertions.assertEquals("CAST(NULL AS Int32?)",
                target0.getColumns().get(8).getComputation().getExpression());
        Assertions.assertEquals(0,
                target0.getColumns().get(8).getComputation().getSources().size());

        // Test MvComputation filter
        Assertions.assertEquals("main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2')", target0.getFilter().getExpression());
        Assertions.assertEquals(2, target0.getFilter().getSources().size());
        Assertions.assertEquals("main", target0.getFilter().getSources().get(0).getAlias());
        Assertions.assertEquals("sub2", target0.getFilter().getSources().get(1).getAlias());

        // Test MvHandler structure
        var handler1 = mc.getHandlers().values().iterator().next();
        Assertions.assertEquals("schema3/h1", handler1.getName());
        Assertions.assertEquals("iddqd", handler1.getConsumerName());

        // Test MvInput structure
        var input1 = handler1.getInputs().get("schema3/main_table");
        Assertions.assertEquals("schema3/main_table", input1.getTableName());
        Assertions.assertEquals("cf1", input1.getChangefeed());
        Assertions.assertFalse(input1.isBatchMode());

        var input2 = handler1.getInputs().get("schema3/sub_table1");
        Assertions.assertEquals("schema3/sub_table1", input2.getTableName());
        Assertions.assertEquals("cf1", input2.getChangefeed());
        Assertions.assertFalse(input2.isBatchMode());

        var input3 = handler1.getInputs().get("schema3/sub_table2");
        Assertions.assertEquals("schema3/sub_table2", input3.getTableName());
        Assertions.assertEquals("cf1", input3.getChangefeed());
        Assertions.assertFalse(input3.isBatchMode());

        var input4 = handler1.getInputs().get("schema3/sub_table3");
        Assertions.assertEquals("schema3/sub_table3", input4.getTableName());
        Assertions.assertEquals("cf1", input4.getChangefeed());
        Assertions.assertTrue(input4.isBatchMode());
    }

    @Test
    public void parserErrorTest1() {
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_BAD1).fill();
        if (PRINT_SQL) {
            new MvIssuePrinter(mc).write(System.out);
        }

        // Test MvContext structure
        Assertions.assertFalse(mc.isValid());
        Assertions.assertEquals(3, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());
    }

    @Test
    public void parserErrorTest2() {
        MvMetadata mc = new MvSqlParser(SqlConstants.SQL_BAD2).fill();
        Assertions.assertTrue(mc.isValid());

        MvTableInfo ti;
        HashMap<String, MvTableInfo> info = new HashMap<>();
        ti = SqlConstants.tiMainTable("schema3/main_table");
        info.put(ti.getName(), ti);
        ti = SqlConstants.tiSubTable1("schema3/sub_table1");
        info.put(ti.getName(), ti);
        ti = SqlConstants.tiSubTable2("schema3/sub_table2");
        info.put(ti.getName(), ti);
        ti = SqlConstants.tiSubTable3("schema3/sub_table3");
        info.put(ti.getName(), ti);
        ti = SqlConstants.tiTarget("schema3/mv99");
        info.put(ti.getName(), ti);

        MvDescriber dummy = new MvDescriber() {
            @Override
            public MvTableInfo describeTable(String tabname) {
                return info.get(tabname);
            }
        };
        mc.linkAndValidate(dummy);

        if (PRINT_SQL) {
            new MvIssuePrinter(mc).write(System.out);
        }

        /*
ERROR: Cannot resolve column reference `main` . `c256` in target MV `schema3/mv99` at position [4:7]
ERROR: Cannot resolve column reference `main` . `badcol` in target MV `schema3/mv99` at position [13:6]
ERROR: Cannot resolve column reference `sub2` . `c77` in target MV `schema3/mv99` at position [13:6]
WARNING: Missing index on columns [c1, c2] for table `schema3/main_table` used as alias `main` in target `schema3/sub_table1_full` at position [6:5]
WARNING: Missing index on columns [c3, c4] for table `schema3/main_table` used as alias `main` in target `schema3/sub_table2_full` at position [6:5]
         */
        Assertions.assertFalse(mc.isValid());
        Assertions.assertEquals(3, mc.getErrors().size());
        Assertions.assertEquals(2, mc.getWarnings().size());
    }

    private void checkJoinCondition(MvJoinCondition cond,
            String firstAlias, String firstColumn, String firstLiteral,
            String secondAlias, String secondColumn, String secondLiteral) {
        if (firstLiteral == null) {
            Assertions.assertNull(cond.getFirstLiteral());
            Assertions.assertEquals(firstAlias, cond.getFirstAlias());
            Assertions.assertEquals(firstColumn, cond.getFirstColumn());
            Assertions.assertNotNull(cond.getFirstRef());
            Assertions.assertEquals(firstAlias, cond.getFirstRef().getTableAlias());
        } else {
            Assertions.assertNotNull(cond.getFirstLiteral());
            Assertions.assertEquals(firstLiteral, cond.getFirstLiteral().getValue());
            Assertions.assertNull(cond.getFirstAlias());
            Assertions.assertNull(cond.getFirstColumn());
            Assertions.assertNull(cond.getFirstRef());
        }
        if (secondLiteral == null) {
            Assertions.assertNull(cond.getSecondLiteral());
            Assertions.assertEquals(secondAlias, cond.getSecondAlias());
            Assertions.assertEquals(secondColumn, cond.getSecondColumn());
            Assertions.assertNotNull(cond.getSecondRef());
            Assertions.assertEquals(secondAlias, cond.getSecondRef().getTableAlias());
        } else {
            Assertions.assertNotNull(cond.getSecondLiteral());
            Assertions.assertEquals(secondLiteral, cond.getSecondLiteral().getValue());
            Assertions.assertNull(cond.getSecondAlias());
            Assertions.assertNull(cond.getSecondColumn());
            Assertions.assertNull(cond.getSecondRef());
        }
    }

}
