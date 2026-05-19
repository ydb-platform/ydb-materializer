package tech.ydb.mv.support;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvName;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.parser.MvDescriberYdb;
import tech.ydb.mv.parser.MvSqlParser;

/**
 * MV configuration reader logic.
 *
 * @author zinal
 */
public class MvConfigReader extends MvName {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvConfigReader.class);

    /**
     * Read metadata configuration.
     *
     * @param ydb YDB connector
     * @param handlerName handler name for modular table reads ({@code null} for
     * full configuration)
     * @return parsed metadata
     */
    public static MvMetadata read(YdbConnector ydb, String handlerName) {
        Properties props = ydb.getConfig().getProperties();
        String mode = props.getProperty(CONF_INPUT_MODE, Input.FILE.name());
        MvMetadata metadata;
        switch (MvName.parseInput(mode)) {
            case FILE -> {
                metadata = readFile(props);
            }
            case TABLE -> {
                metadata = readTable(ydb, props, handlerName);
            }
            default -> {
                throw new IllegalArgumentException("Illegal value [" + mode + "] for "
                        + "property " + MvName.CONF_INPUT_MODE);
            }

        }
        metadata.setDictionaryConsumer(props.getProperty(CONF_DICT_CONSUMER, HANDLER_DICTIONARY));
        return metadata;
    }

    private static MvMetadata readFile(Properties props) {
        String fname = props.getProperty(CONF_INPUT_FILE, DEF_STMT_FILE);
        LOG.info("Reading MV script from file {}", fname);
        try (FileInputStream fis = new FileInputStream(fname)) {
            return new MvSqlParser(fis, StandardCharsets.UTF_8).fill();
        } catch (IOException ix) {
            throw new RuntimeException("Failed to read file [" + fname + "]", ix);
        }
    }

    private static MvMetadata readTable(YdbConnector ydb, Properties props, String handlerName) {
        String tabname = props.getProperty(CONF_INPUT_TABLE, DEF_STMT_TABLE);
        if (handlerName != null && !handlerName.isEmpty()) {
            LOG.info("Reading MV script from table {} for handler {}", tabname, handlerName);
        } else {
            LOG.info("Reading MV script from table {}", tabname);
        }
        String sql = readStatements(ydb, tabname, handlerName);
        if (sql == null || sql.length() == 0) {
            LOG.warn("Empty configuration passed");
            return new MvMetadata();
        }
        return new MvSqlParser(sql).fill();
    }

    private static String readStatements(YdbConnector ydb, String tabname, String handlerName) {
        boolean modular = isModularStatementsTable(ydb, tabname);
        if (modular) {
            LOG.debug("Statements table {} uses modular format (column {})", tabname, STMT_COL_MODULE_ID);
        } else {
            LOG.debug("Statements table {} uses legacy format", tabname);
        }
        StatementsQuery query = buildStatementsQuery(tabname, handlerName, modular);
        var result = ydb.sqlRead(query.sql, query.params).getResultSet(0);
        final StringBuilder sb = new StringBuilder();
        while (result.next()) {
            sb.append(result.getColumn(0).getText());
            sb.append("\n");
        }
        return sb.toString();
    }

    static boolean isModularStatementsTable(MvTableInfo tableInfo) {
        return tableInfo != null && tableInfo.getColumns().containsKey(STMT_COL_MODULE_ID);
    }

    private static boolean isModularStatementsTable(YdbConnector ydb, String tabname) {
        MvTableInfo tableInfo = new MvDescriberYdb(ydb).describeTable(tabname, null);
        return isModularStatementsTable(tableInfo);
    }

    static StatementsQuery buildStatementsQuery(String tabname, String handlerName, boolean modular) {
        String safeName = safe(tabname);
        if (!modular) {
            return new StatementsQuery(
                    "SELECT statement_text, statement_no FROM `" + safeName + "` ORDER BY statement_no",
                    Params.empty());
        }
        if (handlerName == null || handlerName.isEmpty()) {
            return new StatementsQuery(
                    "SELECT statement_text, statement_no, " + STMT_COL_MODULE_ID
                    + " FROM `" + safeName + "` ORDER BY "
                    + STMT_COL_MODULE_ID + ", statement_no",
                    Params.empty());
        }
        return new StatementsQuery(
                "SELECT statement_text, statement_no, " + STMT_COL_MODULE_ID
                + " FROM `" + safeName + "` WHERE "
                + STMT_COL_MODULE_ID + " IN ($root, $handler) ORDER BY "
                + STMT_COL_MODULE_ID + ", statement_no",
                Params.of(
                        "$root", PrimitiveValue.newText(STMT_MODULE_ROOT),
                        "$handler", PrimitiveValue.newText(handlerName)));
    }

    /**
     * SQL text and optional parameters for loading statements from a table.
     */
    static final class StatementsQuery {

        final String sql;
        final Params params;

        StatementsQuery(String sql, Params params) {
            this.sql = sql;
            this.params = params;
        }
    }

}
