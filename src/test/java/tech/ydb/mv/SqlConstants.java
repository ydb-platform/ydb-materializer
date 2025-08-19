package tech.ydb.mv;

import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.table.values.PrimitiveType;

/**
 *
 * @author zinal
 */
public class SqlConstants {

    public static final String SQL_GOOD1 =
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
INNER JOIN sub_table3 AS sub3
  ON sub3.c5=58
WHERE COMPUTE ON main, sub2
#[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2') ]#;

CREATE ASYNC HANDLER h1
  PROCESS m1,
  INPUT main_table CHANGEFEED cf1 AS STREAM,
  INPUT sub_table1 CHANGEFEED cf1 AS STREAM,
  INPUT sub_table2 CHANGEFEED cf1 AS STREAM,
  INPUT sub_table3 CHANGEFEED cf1 AS BATCH;
""";

    public static final String SQL_GOOD2 =
"""
-- async view demo (with something strange within)
CREATE ASYNC MATERIALIZED VIEW `schema3/mv1` AS
SELECT main.id AS id, main.c1 AS c1, main . c2 AS c2, main . c3 AS c3,
       sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10,
       COMPUTE ON main #[ Substring(main.c20,3,5) ]# AS c11,
       COMPUTE #[ CAST(NULL AS Int32?) ]# AS c12
FROM `schema3/main_table` AS main /* something */
INNER JOIN `schema3/sub_table1` AS sub1
  ON main.c1=sub1.c1 AND main.c2=sub1.c2
LEFT JOIN `schema3/sub_table2` AS sub2
  ON main.c3=sub2.c3 AND main.c4=sub2.c4
INNER JOIN `schema3/sub_table3` AS sub3
  ON sub3.c5=sub2.c5
WHERE COMPUTE ON main, sub2
#[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2') ]#;

CREATE ASYNC HANDLER `schema3/h1`
  PROCESS `schema3/mv1`,
  INPUT `schema3/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `schema3/sub_table1` CHANGEFEED cf1 AS STREAM,
  INPUT `schema3/sub_table2` CHANGEFEED cf1 AS STREAM,
  INPUT `schema3/sub_table3` CHANGEFEED cf1 AS BATCH;
""";

    public static final String SQL_BAD1 =
"""
CREATE ASYNC MATERIALIZED VIEW m1 AS
SELECT main.id AS id, main.c1 AS c1, main . c2 AS c2, main . c3 AS c3,
       sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10,
       COMPUTE ON main #[ Substring(main.c20,3,5) ]# AS c11
FROM main_table AS main /* something */
INNER JOIN sub_table1 AS sub1
  ON main.c1=sub1.c1 AND main.c2=sub1.c2
RIGHT JOIN sub_table2 AS sub2
  ON main.c3=sub2.c3 AND 'val1'=sub2.c4
INNER JOIN sub_table3 AS sub3
  ON sub3.c5=58
WHERE COMPUTE ON main, sub2
#[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2') ]#;
""";

    public static MvTableInfo tiMainTable(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("id", PrimitiveType.Int32)
                .addColumn("c1", PrimitiveType.Int32)
                .addColumn("c2", PrimitiveType.Int32)
                .addColumn("c3", PrimitiveType.Int32)
                .addColumn("c4", PrimitiveType.Int32)
                .addColumn("c6", PrimitiveType.Int32)
                .addColumn("c20", PrimitiveType.Text)
                .addKey("id")
                .build();
    }

    public static MvTableInfo tiSubTable1(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("c1", PrimitiveType.Int32)
                .addColumn("c2", PrimitiveType.Int32)
                .addColumn("c8", PrimitiveType.Text)
                .addKey("c1")
                .addKey("c2")
                .build();
    }

    public static MvTableInfo tiSubTable2(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("c3", PrimitiveType.Int32)
                .addColumn("c4", PrimitiveType.Int32)
                .addColumn("c5", PrimitiveType.Int32)
                .addColumn("c7", PrimitiveType.Text)
                .addColumn("c9", PrimitiveType.Text)
                .addKey("c3")
                .build();
    }

    public static MvTableInfo tiSubTable3(String name) {
        return MvTableInfo.newBuilder(name)
                .addColumn("c5", PrimitiveType.Int32)
                .addColumn("c10", PrimitiveType.Text)
                .addKey("c5")
                .build();
    }
}
