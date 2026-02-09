package tech.ydb.mv.parser;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import tech.ydb.table.values.PrimitiveType;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvViewExpr;

/**
 * Tests for computed primary key extraction with UNION-like patterns.
 */
public class MvKeyPathComputedPkTest {

    @Test
    public void testComputedKeyPathFromSubTable() {
        MvTableInfo mainTable = MvTableInfo.newBuilder("main_table1")
                .addColumn("id", PrimitiveType.Text)
                .addColumn("payload", PrimitiveType.Text)
                .addKey("id")
                .build();

        MvTableInfo subTable = MvTableInfo.newBuilder("sub_table1")
                .addColumn("main_id", PrimitiveType.Text)
                .addColumn("value", PrimitiveType.Text)
                .addKey("main_id")
                .build();

        MvTableInfo viewTable = MvTableInfo.newBuilder("complex_view")
                .addColumn("kind", PrimitiveType.Text)
                .addColumn("id", PrimitiveType.Text)
                .addColumn("value", PrimitiveType.Text)
                .addKey("kind")
                .addKey("id")
                .build();

        MvJoinSource main = new MvJoinSource();
        main.setTableName("main_table1");
        main.setTableAlias("m");
        main.setMode(MvJoinMode.MAIN);
        main.setTableInfo(mainTable);

        MvJoinSource sub = new MvJoinSource();
        sub.setTableName("sub_table1");
        sub.setTableAlias("s");
        sub.setMode(MvJoinMode.INNER);
        sub.setTableInfo(subTable);

        MvJoinCondition join = new MvJoinCondition();
        join.setFirstRef(main);
        join.setFirstAlias("m");
        join.setFirstColumn("id");
        join.setSecondRef(sub);
        join.setSecondAlias("s");
        join.setSecondColumn("main_id");
        sub.getConditions().add(join);

        MvViewExpr target = new MvViewExpr("complex_view");
        target.setTableInfo(viewTable);
        target.getSources().add(main);
        target.getSources().add(sub);

        MvColumn kind = new MvColumn("kind");
        kind.setComputation(new MvComputation("'first'u"));
        kind.setType(PrimitiveType.Text);
        target.getColumns().add(kind);

        MvColumn id = new MvColumn("id");
        id.setSourceAlias("m");
        id.setSourceColumn("id");
        id.setSourceRef(main);
        id.setType(PrimitiveType.Text);
        target.getColumns().add(id);

        MvColumn value = new MvColumn("value");
        value.setSourceAlias("s");
        value.setSourceColumn("value");
        value.setSourceRef(sub);
        value.setType(PrimitiveType.Text);
        target.getColumns().add(value);

        MvViewExpr result = new MvPathGenerator(target).extractKeysReverse(sub);
        assertNotNull(result);

        String sql;
        try (MvSqlGen sg = new MvSqlGen(result)) {
            sql = sg.makeSelect();
        }
        assertTrue(sql.contains("'first'u AS kind"),
                "SQL should compute literal key column");
        assertTrue(sql.contains("s.main_id AS id"),
                "SQL should map main key through input table");
    }
}
