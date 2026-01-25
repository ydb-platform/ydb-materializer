package tech.ydb.mv.parser;

import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.query.QuerySession;

/**
 *
 * @author zinal
 */
public class MvStreamBuilder {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvStreamBuilder.class);

    private final YdbConnector conn;
    private final PrintStream pw;
    private final MvHandler handler;
    private final boolean create;

    public MvStreamBuilder(YdbConnector conn, PrintStream pw, MvHandler handler, boolean create) {
        this.conn = conn;
        this.pw = pw;
        this.handler = handler;
        this.create = create;
    }

    public void apply() {
        LOG.info("Stream object analysis started for handler {}", handler.getName());
        for (var input : handler.getInputs().values()) {
            apply(input);
        }
        LOG.info("Stream object analysis completed for handler {}", handler.getName());
    }

    private void apply(MvInput input) {
        String sql = generateStreamSql(input);
        pw.println(sql);
        pw.println();
        var cf = input.getTableInfo().getChangefeeds().get(input.getChangefeed());
        if (create && cf == null) {
            runDdl(sql);
        }
        String consumerName = handler.getConsumerNameAlways();
        sql = generateConsumerSql(input, consumerName);
        pw.println(sql);
        pw.println();
        if (create && ((cf == null) || !cf.getConsumers().contains(consumerName))) {
            runDdl(sql);
        }
    }

    private String generateStreamSql(MvInput input) {
        var sb = new StringBuilder();
        sb.append("ALTER TABLE `")
                .append(input.getTableName())
                .append("` ADD CHANGEFEED `")
                .append(input.getChangefeed())
                .append("` WITH (FORMAT='JSON', MODE='")
                .append(input.isBatchMode() ? "NEW_AND_OLD_IMAGES" : "KEYS_ONLY")
                .append("');");
        return sb.toString();
    }

    private String generateConsumerSql(MvInput input, String consumerName) {
        var sb = new StringBuilder();
        sb.append("ALTER TOPIC `")
                .append(input.getTableName())
                .append('/')
                .append(input.getChangefeed())
                .append("` ADD CONSUMER `")
                .append(consumerName)
                .append("`;");
        return sb.toString();
    }

    private void runDdl(String sql) {
        var status = conn.getQueryRetryCtx()
                .supplyStatus(qs -> runSql(qs, sql, TxMode.NONE))
                .join();
        if (status.isSuccess()) {
            LOG.info("SQL: {}", sql);
        } else {
            LOG.error("SQL FAILED: {}\n\t{}", sql, status);
        }
    }

    private static CompletableFuture<Status> runSql(QuerySession qs, String sql, TxMode txMode) {
        return qs.createQuery(sql, txMode)
                .execute()
                .thenApply(res -> res.getStatus());
    }

}
