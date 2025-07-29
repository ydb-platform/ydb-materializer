package tech.ydb.mv.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.SqlConstants;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvIssue;

/**
 *
 * @author zinal
 */
public class ParseErrorTest {

    @Test
    public void parserTest() {
        MvContext mc = new MvParser(SqlConstants.SQL2).fill();

        // Test MvContext structure
        Assertions.assertFalse(mc.isValid());
        Assertions.assertEquals(3, mc.getErrors().size());
        Assertions.assertEquals(0, mc.getWarnings().size());

        for (MvIssue i : mc.getErrors()) {
            System.out.println("\t" + i.getMessage());
        }
    }

}
