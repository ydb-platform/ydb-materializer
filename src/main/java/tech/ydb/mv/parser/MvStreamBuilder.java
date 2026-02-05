package tech.ydb.mv.parser;

import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.query.QuerySession;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvMetadata;

/**
 *
 * @author zinal
 */
public class MvStreamBuilder {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvStreamBuilder.class);

    private final YdbConnector conn;
    private final MvMetadata metadata;
    private final MvHandler handler;
    private final PrintStream pw;
    private final boolean create;

    public MvStreamBuilder(YdbConnector conn, MvMetadata metadata, MvHandler handler,
            PrintStream pw, boolean create) {
        this.conn = conn;
        this.metadata = metadata;
        this.handler = handler;
        this.pw = pw;
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
        if (input.isBatchMode()) {
            consumerName = metadata.getDictionaryConsumer();
        }
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
