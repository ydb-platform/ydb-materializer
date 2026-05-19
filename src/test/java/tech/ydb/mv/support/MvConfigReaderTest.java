package tech.ydb.mv.support;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveType;

import tech.ydb.mv.MvName;
import tech.ydb.mv.model.MvTableInfo;

/**
 *
 * @author zinal
 */
public class MvConfigReaderTest {

    @Test
    public void detectModularFormat() {
        MvTableInfo legacy = MvTableInfo.newBuilder("t")
                .addColumn("statement_no", PrimitiveType.Int32)
                .addColumn("statement_text", PrimitiveType.Text)
                .addKey("statement_no")
                .build();
        Assertions.assertFalse(MvConfigReader.isModularStatementsTable(legacy));

        MvTableInfo modular = MvTableInfo.newBuilder("t")
                .addColumn(MvName.STMT_COL_MODULE_ID, PrimitiveType.Text)
                .addColumn("statement_no", PrimitiveType.Int32)
                .addColumn("statement_text", PrimitiveType.Text)
                .addKey(MvName.STMT_COL_MODULE_ID)
                .addKey("statement_no")
                .build();
        Assertions.assertTrue(MvConfigReader.isModularStatementsTable(modular));
        Assertions.assertFalse(MvConfigReader.isModularStatementsTable(null));
    }

    @Test
    public void buildLegacyQuery() {
        MvConfigReader.StatementsQuery q = MvConfigReader.buildStatementsQuery(
                "mv/statements", "handler1", false);
        Assertions.assertEquals(
                "SELECT statement_text, statement_no FROM `mv/statements` ORDER BY statement_no",
                q.sql);
        Assertions.assertEquals(Params.empty(), q.params);

        q = MvConfigReader.buildStatementsQuery("mv/statements", "handler1", false);
        Assertions.assertEquals(q.sql, MvConfigReader.buildStatementsQuery(
                "mv/statements", null, false).sql);
    }

    @Test
    public void buildModularQueryFull() {
        MvConfigReader.StatementsQuery q = MvConfigReader.buildStatementsQuery(
                "mv/statements", null, true);
        Assertions.assertEquals(
                "SELECT statement_text, statement_no, " + MvName.STMT_COL_MODULE_ID
                + " FROM `mv/statements` ORDER BY "
                + MvName.STMT_COL_MODULE_ID + ", statement_no",
                q.sql);
        Assertions.assertEquals(Params.empty(), q.params);
    }

    @Test
    public void buildModularQueryHandler() {
        MvConfigReader.StatementsQuery q = MvConfigReader.buildStatementsQuery(
                "mv/statements", "handler1", true);
        Assertions.assertEquals(
                "SELECT statement_text, statement_no, " + MvName.STMT_COL_MODULE_ID
                + " FROM `mv/statements` WHERE "
                + MvName.STMT_COL_MODULE_ID + " IN ($root, $handler) ORDER BY "
                + MvName.STMT_COL_MODULE_ID + ", statement_no",
                q.sql);
        Assertions.assertNotEquals(Params.empty(), q.params);
    }

}
