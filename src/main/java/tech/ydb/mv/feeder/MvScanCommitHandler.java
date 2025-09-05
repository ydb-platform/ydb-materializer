package tech.ydb.mv.feeder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.query.QuerySession;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.model.MvKey;

/**
 * Scan commit handler writes the key position to the database table.
 *
 * @author zinal
 */
public class MvScanCommitHandler implements MvCommitHandler {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvScanCommitHandler.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);

    private final long instance;
    private final MvScanContext context;
    private final String jsonKey;
    private volatile int counter;
    private volatile boolean committed;
    private final AtomicReference<MvScanCommitHandler> previous;
    private final AtomicReference<MvScanCommitHandler> next;
    private final boolean terminal;

    public MvScanCommitHandler(MvScanContext context, MvKey key,
            int initialCount, MvScanCommitHandler predecessor, boolean terminal) {
        this.instance = COUNTER.incrementAndGet();
        this.context = context;
        this.jsonKey = (key==null) ? "" : key.convertKeyToJson();
        this.counter = initialCount;
        this.committed = false;
        this.previous = new AtomicReference<>(predecessor);
        this.next = new AtomicReference<>();
        this.terminal = terminal;
        initPredecessor(predecessor);
        LOG.debug("instance {} created -> {}", instance, counter);
    }

    @Override
    public long getInstance() {
        return instance;
    }

    @Override
    public synchronized int getCounter() {
        return counter;
    }

    private void initPredecessor(MvScanCommitHandler predecessor) {
        if (predecessor!=null) {
            predecessor.next.set(this);
        }
    }

    @Override
    public synchronized void commit(int count) {
        if (committed || counter < 0) {
            return;
        }
        if (! context.isRunning()) {
            // no commits for an already stopped scan feeder
            committed = true;
            resetNextChain();
            LOG.debug("instance {} reset due to context stop", instance);
            return;
        }
        counter -= Math.min(count, counter);
        LOG.debug("instance {} commit {} -> {}", instance, count, counter);
        if (isReady()) {
            committed = true;
            LOG.debug("instance {} commit APPLY", instance);
            try {
                context.getRetryCtx().supplyStatus(qs -> doApply(qs))
                        .join().expectSuccess();
            } catch(Exception ex) {
                LOG.warn("Failed to commit the offset in scan feeder for target {}, handler {}",
                        context.getTarget().getName(), context.getHandler().getName(), ex);
            }
            resetNext();
        }
    }

    @Override
    public synchronized void reserve(int count) {
        if (count > 0 && !committed) {
            counter += count;
            LOG.debug("instance {} reserve {} -> {}", instance, count, counter);
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
            n.commit(0);
        }
        return n;
    }

    private void resetNextChain() {
        MvScanCommitHandler n = this;
        while (n!=null) {
            n = n.resetNext();
        }
    }

    private CompletableFuture<Status> doApply(QuerySession qs) {
        Params params;
        String sqlText;
        if (terminal) {
            LOG.info("Performing final commit in scan feeder for target {}, handler {}",
                    context.getTarget().getName(), context.getHandler().getName());
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
            if (p.getCounter() > 0) {
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

    @Override
    public String toString() {
        return "MvScanCommitHandler{" + instance + '}';
    }

}
