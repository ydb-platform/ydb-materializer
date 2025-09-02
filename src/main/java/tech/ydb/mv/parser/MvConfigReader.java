package tech.ydb.mv.parser;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvContext;
import tech.ydb.table.query.Params;

/**
 * MV configuration reader logic.
 *
 * @author zinal
 */
public class MvConfigReader extends MvConfig {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvConfigReader.class);

    public static MvContext readContext(YdbConnector ydb, Properties props) {
        String mode = props.getProperty(CONF_INPUT_MODE, Input.FILE.name());
        switch (MvConfig.parseInput(mode)) {
            case FILE -> { return readFile(ydb, props); }
            case TABLE -> { return readTable(ydb, props); }
        }
        throw new IllegalArgumentException("Illegal value [" + mode + "] for "
                + "property " + MvConfig.CONF_INPUT_MODE);
    }

    private static MvContext readFile(YdbConnector ydb, Properties props) {
        String fname = props.getProperty(CONF_INPUT_FILE, DEF_STMT_FILE);
        LOG.info("Reading MV script from file {}", fname);
        try (FileInputStream fis = new FileInputStream(fname)) {
            return new MvSqlParser(fis, StandardCharsets.UTF_8).fill();
        } catch(IOException ix) {
            throw new RuntimeException("Failed to read file [" + fname + "]", ix);
        }
    }

    private static MvContext readTable(YdbConnector ydb, Properties props) {
        String tabname = props.getProperty(CONF_INPUT_TABLE, DEF_STMT_TABLE);
        LOG.info("Reading MV script from table {}", tabname);
        String sql = readStatements(ydb, tabname);
        return new MvSqlParser(sql).fill();
    }

    private static String readStatements(YdbConnector ydb, String tabname) {
        String stmt = "SELECT statement_text, statement_no FROM `"
                + YdbConnector.safe(tabname) + "` ORDER BY statement_no";
        var result = ydb.sqlRead(stmt, Params.empty()).getResultSet(0);
        final StringBuilder sb = new StringBuilder();
        while (result.next()) {
            sb.append(result.getColumn(0).getText());
            sb.append("\n");
        }
        return sb.toString();
    }

}
