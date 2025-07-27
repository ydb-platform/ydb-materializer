package tech.ydb.mv.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.model.MvContext;

/**
 *
 * @author zinal
 */
public class ParseErrorTest {

    private static final String SQL1 =
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

    @Test
    public void parserTest() {
        MvContext mc = new MvParser(SQL1).fill();

        // Test MvContext structure
        Assertions.assertFalse(mc.isValid());
        Assertions.assertEquals(3, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());

        System.out.println("Errors: " + mc.getErrors());
    }

}
