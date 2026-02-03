package tech.ydb.mv.apply;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.settings.ExecuteQuerySettings;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;

import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.metrics.MvMetrics;
import tech.ydb.mv.parser.MvSqlGen;

/**
 * Common parts of action handlers in the form of a base class.
 *
 * @author zinal
 */
abstract class ActionBase {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ActionBase.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);

    protected final long instance;
    protected final MvActionContext context;
    protected final MvApplyManager applyManager;
    protected final SessionRetryContext retryCtx;
    protected static final ThreadLocal<String> lastSqlStatement = new ThreadLocal<>();
    protected final ExecuteQuerySettings querySettings;
    private String metricsType;
    private String metricsTarget;
    private String metricsAlias;
    private String metricsSource;
    private String metricsItem;

    protected ActionBase(MvActionContext context) {
        this.instance = COUNTER.incrementAndGet();
        this.context = context;
        this.applyManager = context.getApplyManager();
        this.retryCtx = context.getRetryCtx();
        var queryTimeout = context.getSettings().getQueryTimeoutSeconds();
        this.querySettings = ExecuteQuerySettings.newBuilder()
                .withRequestTimeout(Duration.ofSeconds(queryTimeout))
                .build();
    }

    protected final void setMetricsScope(String type, String target,
                                         String alias, String source, String item) {
        this.metricsType = type;
        this.metricsTarget = target;
        this.metricsAlias = alias;
        this.metricsSource = source;
        this.metricsItem = item;
    }

    String getMetricsType() {
        return metricsType;
    }

    String getMetricsTarget() {
        return metricsTarget;
    }

    String getMetricsAlias() {
        return metricsAlias;
    }

    String getMetricsSource() {
        return metricsSource;
    }

    String getMetricsItem() {
        return metricsItem;
    }

    public static String getLastSqlStatement() {
        return lastSqlStatement.get();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 89 * hash + (int) (this.instance ^ (this.instance >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ActionBase other = (ActionBase) obj;
        return this.instance == other.instance;
    }

    protected String getSqlSelect() {
        throw new UnsupportedOperationException();
    }

    protected final ResultSetReader readTaskRows(List<MvApplyTask> tasks) {
        return readRows(tasks.stream()
                .map(task -> task.getData().getKey())
                .distinct()
                .toList());
    }

    protected final ResultSetReader readRows(List<MvKey> items) {
        String statement = getSqlSelect();
        Value<?> keys = keysToParam(items);
        if (LOG.isDebugEnabled()) {
            LOG.debug("SELECT via statement << {} >>, keys {}", statement, keys);
        }
        Params params = Params.of(MvSqlGen.SYS_KEYS_VAR, keys);
        lastSqlStatement.set(statement);
        long startNs = System.nanoTime();
        ResultSetReader rsr = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(statement, TxMode.SNAPSHOT_RO, params, querySettings)
        )).join().getValue().getResultSet(0);
        long durationNs = System.nanoTime() - startNs;
        if (metricsTarget != null) {
            MvMetrics.recordSqlTime(metricsType, metricsTarget, metricsAlias,
                    "select", durationNs);
        }
        lastSqlStatement.set(null);
        return rsr;
    }

    protected static Value<?> keysToParam(List<MvKey> items) {
        StructValue[] values = items.stream()
                .map(item -> item.convertKeyToStructValue())
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    protected static Value<?> structsToParam(List<StructValue> items) {
        StructValue[] values = items.stream()
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    protected final int getReadBatchSize() {
        int readBatchSize = context.getSettings().getSelectBatchSize();
        if (readBatchSize < 1) {
            readBatchSize = 1;
        }
        return readBatchSize;
    }

    protected final int getWriteBatchSize() {
        int readBatchSize = getReadBatchSize();
        int writeBatchSize = context.getSettings().getUpsertBatchSize();
        if (writeBatchSize > readBatchSize) {
            writeBatchSize = readBatchSize;
        }
        return writeBatchSize;
    }

    /**
     * Group the input records by commit handlers. This enables more efficient
     * per-commit-handler behavior.
     */
    protected static class PerCommit {

        final HashMap<MvCommitHandler, ArrayList<MvApplyTask>> items = new HashMap<>();

        PerCommit(List<MvApplyTask> tasks) {
            for (MvApplyTask task : tasks) {
                ArrayList<MvApplyTask> cur = items.get(task.getCommit());
                if (cur == null) {
                    cur = new ArrayList<>();
                    items.put(task.getCommit(), cur);
                }
                cur.add(task);
            }
        }

        public void apply(BiConsumer<MvCommitHandler, List<MvApplyTask>> consumer) {
            items.forEach((handler, tasks) -> consumer.accept(handler, tasks));
        }
    }
}
