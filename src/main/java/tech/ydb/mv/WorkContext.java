package tech.ydb.mv;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.parser.MvParser;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * Work context for YDB Materializer activities.
 * @author mzinal
 */
public class WorkContext implements AutoCloseable {

    private final YdbConnector connector;
    private final MvContext context;

    public WorkContext(YdbConnector.Config config) {
        this.connector = new YdbConnector(config);
        this.context = readContext(this.connector);
        refreshMetadata();
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvContext getContext() {
        return context;
    }

    @Override
    public void close() {
        connector.close();
    }

    private void refreshMetadata() {

    }

    private static MvContext readContext(YdbConnector ydb) {
        String mode = ydb.getProperty(App.CONF_INPUT_MODE, App.Input.FILE.name());
        switch (App.parseInput(mode)) {
            case FILE -> { return readFile(ydb); }
            case TABLE -> { return readTable(ydb); }
        }
        throw new IllegalArgumentException("Illegal value [" + mode + "] for "
                + "property " + App.CONF_INPUT_MODE);
    }

    private static MvContext readFile(YdbConnector ydb) {
        String fname = ydb.getProperty(App.CONF_INPUT_FILE, App.DEF_FILE);
        try (FileInputStream fis = new FileInputStream(fname)) {
            return new MvParser(fis, StandardCharsets.UTF_8).fill();
        } catch(IOException ix) {
            throw new RuntimeException("Failed to read file [" + fname + "]", ix);
        }
    }

    private static MvContext readTable(YdbConnector ydb) {
        String tabname = ydb.getProperty(App.CONF_INPUT_TABLE, App.DEF_TABLE);
        String sql = readStatements(ydb, tabname);
        return new MvParser(sql).fill();
    }

    private static String readStatements(YdbConnector ydb, String tabname) {
        var retryCtx = SessionRetryContext
                .create(ydb.getQueryClient())
                .idempotent(true)
                .build();
        String stmt = "SELECT statement_text, statement_no FROM `"
                + YdbConnector.safe(tabname) + "` ORDER BY statement_no";
        var result = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(stmt, TxMode.ONLINE_RO)
        )).join().getValue().getResultSet(0);
        final StringBuilder sb = new StringBuilder();
        while (result.next()) {
            sb.append(result.getColumn(0).getText());
            sb.append("\n");
        }
        return sb.toString();
    }

}
