package tech.ydb.mv.feeder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.query.QuerySession;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.apply.MvCommitHandler;
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
    private final MvScanContext context;
    private final String jsonKey;
    private final AtomicInteger counter;
    private final AtomicReference<MvScanCommitHandler> previous;
    private final AtomicReference<MvScanCommitHandler> next;
    private final boolean terminal;

    public MvScanCommitHandler(MvScanContext context, MvKey key,
            int initialCount, MvScanCommitHandler predecessor, boolean terminal) {
        this.instance = COUNTER.incrementAndGet();
        this.context = context;
        this.jsonKey = (key==null) ? "" : key.convertKeyToJson();
        this.counter = new AtomicInteger(initialCount);
        this.previous = new AtomicReference<>(predecessor);
        this.next = new AtomicReference<>();
        this.terminal = terminal;
        initPredecessor(predecessor);
    }

    private void initPredecessor(MvScanCommitHandler predecessor) {
        if (predecessor!=null) {
            predecessor.next.set(this);
        }
    }

    private MvScanCommitHandler resetNext() {
        MvScanCommitHandler n = next.getAndSet(null);
        if (n!=null) {
            MvScanCommitHandler p = n.previous.getAndSet(null);
            if (p!=this) {
                LOG.warn("Commit handler chain mismatched: expected {}, got {}",
                        instance, p.instance);
            }
        }
        return n;
    }

    private void resetNextChain() {
        MvScanCommitHandler n = this;
        while (n!=null) {
            n = n.resetNext();
        }
    }

    @Override
    public void apply(int count) {
        if (! context.isRunning()) {
            // no commits for an already stopped scan feeder
            resetNextChain();
            return;
        }
        counter.addAndGet(-1 * count);
        if (isReady()) {
            try {
                context.getRetryCtx().supplyStatus(qs -> doApply(qs))
                        .join().expectSuccess();
            } catch(Exception ex) {
                LOG.warn("Failed to commit the offset", ex);
            }
            resetNext();
        }
    }

    private CompletableFuture<Status> doApply(QuerySession qs) {
        Params params;
        String sqlText;
        if (terminal) {
            sqlText = context.getSqlPosDelete();
            params = Params.of(
                    "$handler_name", context.getHandlerName(),
                    "$table_name", context.getTargetName()
            );
        } else {
            sqlText = context.getSqlPosUpsert();
            params = Params.of(
                    "$handler_name", context.getHandlerName(),
                    "$table_name", context.getTargetName(),
                    "$key_position", PrimitiveValue.newText(jsonKey)
            );
        }
        return qs.createQuery(sqlText, TxMode.SERIALIZABLE_RW, params)
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
