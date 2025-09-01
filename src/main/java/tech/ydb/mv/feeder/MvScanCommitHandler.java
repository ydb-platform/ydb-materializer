package tech.ydb.mv.feeder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.query.QuerySession;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.apply.MvCommitHandler;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvKey;

/**
 * Scan commit handler writes the key position to the database table.
 *
 * @author zinal
 */
class MvScanCommitHandler implements MvCommitHandler {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvScanCommitHandler.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);

    private final long instance;
    private final MvHandler handler;
    private final MvKey key;
    private final String jsonKey;
    private final AtomicInteger counter;
    private final AtomicReference<MvScanCommitHandler> previous;
    private final AtomicReference<MvScanCommitHandler> next;
    private final SessionRetryContext retryCtx;
    private final String upsertSql;

    public MvScanCommitHandler(YdbConnector ydb, MvHandler handler, MvKey key,
            int initialCount, MvScanCommitHandler predecessor) {
        this.instance = COUNTER.incrementAndGet();
        this.handler = handler;
        this.key = key;
        this.jsonKey = key.convertKeyToJson();
        this.counter = new AtomicInteger(initialCount);
        this.previous = new AtomicReference<>(predecessor);
        this.next = new AtomicReference<>();
        this.retryCtx = ydb.getQueryRetryCtx();
        this.upsertSql = buildSql(ydb.getProperty(MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE));
        initPredecessor(predecessor);
    }

    private void initPredecessor(MvScanCommitHandler predecessor) {
        if (predecessor!=null) {
            predecessor.next.set(this);
        }
    }

    private void resetNext() {
        MvScanCommitHandler n = next.getAndSet(null);
        if (n!=null) {
            MvScanCommitHandler p = n.previous.getAndSet(null);
            if (p!=this) {
                LOG.warn("Commit handler chain mismatched: expected {}, got {}",
                        instance, p.instance);
            }
        }
    }

    @Override
    public void apply(int count) {
        counter.addAndGet(-1 * count);
        if (isReady()) {
            try {
                retryCtx.supplyStatus(qs -> doApply(qs))
                        .join().expectSuccess();
            } catch(Exception ex) {
                LOG.warn("Failed to commit the offset", ex);
            }
            resetNext();
        }
    }

    private static String buildSql(String workTableName) {
        return "DECLARE $handler_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "DECLARE $key_position AS JsonDocument; "
                + "UPSERT INTO `" + workTableName + "` "
                + "(handler_name, table_name, updated_at, key_position) "
                + "VALUES ($handler_name, $table_name, CurrentUtcTimestamp(), $key_position);";
    }

    private CompletableFuture<Status> doApply(QuerySession qs) {
        Params params = Params.of(
                "$handler_name", PrimitiveValue.newText(handler.getName()),
                "$table_name", PrimitiveValue.newText(key.getTableInfo().getName()),
                "$key_position", PrimitiveValue.newText(jsonKey)
        );
        return qs.createQuery(upsertSql, TxMode.SERIALIZABLE_RW, params)
                .execute()
                .thenApply(qi -> qi.getStatus());
    }

    private boolean isReady() {
        MvScanCommitHandler p = this;
        while (p!=null) {
            if (p.counter.get() > 0) {
                return false;
            }
            p = p.previous.get();
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (int) (this.instance ^ (this.instance >>> 32));
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
        final MvScanCommitHandler other = (MvScanCommitHandler) obj;
        return this.instance == other.instance;
    }

}
