package tech.ydb.mv.feeder;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.data.MvKey;

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
    private final MvKey key;
    private final AtomicInteger counter;
    private final AtomicBoolean committed = new AtomicBoolean(false);
    private final AtomicReference<MvScanCommitHandler> previous;
    private final AtomicReference<MvScanCommitHandler> next;
    private final boolean terminal;

    public MvScanCommitHandler(MvScanContext context, MvKey key,
            int initialCount, MvScanCommitHandler predecessor, boolean terminal) {
        this.instance = COUNTER.incrementAndGet();
        this.context = context;
        this.key = key;
        this.counter = new AtomicInteger(initialCount);
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
    public int getCounter() {
        return counter.get();
    }

    private void initPredecessor(MvScanCommitHandler predecessor) {
        if (predecessor != null) {
            predecessor.next.set(this);
        }
    }

    @Override
    public void reserve(int count) {
        if (count > 0 && !committed.get()) {
            int value = counter.addAndGet(count);
            LOG.debug("instance {} reserve {} -> {}", instance, count, value);
        }
    }

    @Override
    public void commit(int count) {
        if (committed.get() || counter.get() < 0) {
            return;
        }
        if (!context.isRunning()) {
            // no commits for an already stopped scan feeder
            committed.set(true);
            resetFullChain();
            LOG.debug("instance {} reset due to context stop", instance);
            return;
        }
        // Iterate through the chain without recursion to avoid stack overflow on long chains
        var currentHandler = this;
        int remainingCount = count;
        while (currentHandler != null) {
            var nextHandler = currentHandler.doCommit(remainingCount);
            if (nextHandler == null) {
                break;
            }
            currentHandler = nextHandler;
            remainingCount = 0; // zero for all but the first handler
        }
    }

    /**
     * Performs commit for this handler only. Returns the next handler to
     * process if this handler committed and there is a successor, or null to
     * stop propagation.
     */
    private MvScanCommitHandler doCommit(int count) {
        if (committed.get() || counter.get() < 0) {
            return null;
        }
        if (!context.isRunning()) {
            committed.set(true);
            return null;
        }
        int value = counter.updateAndGet(v -> (v > count) ? v - count : 0);
        LOG.debug("instance {} commit {} -> {}", instance, count, value);
        if (!isReady()) {
            return null;
        }
        committed.set(true);
        LOG.debug("instance {} commit APPLY", instance);
        try {
            if (terminal) {
                LOG.info("Final commit for scan feeder of target `{}` as {} in handler `{}`",
                        context.getTarget().getName(), context.getTarget().getAlias(),
                        context.getHandler().getName());
                context.getScanDao().unregisterScan();
            } else {
                context.getScanDao().saveScan(key);
            }
        } catch (Exception ex) {
            LOG.error("Failed to commit the scan feeder for target `{}` as {} in handler `{}`",
                    context.getTarget().getName(), context.getTarget().getAlias(),
                    context.getHandler().getName(), ex);
        }
        return detachNext();
    }

    /**
     * Detaches the next handler from the chain and returns it. Does not invoke
     * commit.
     */
    private MvScanCommitHandler detachNext() {
        MvScanCommitHandler n = next.getAndSet(null);
        if (n != null) {
            MvScanCommitHandler p = n.previous.getAndSet(null);
            if (p != this) {
                LOG.warn("Commit handler chain mismatched: expected {}, got {}",
                        instance, p != null ? p.instance : "null");
            }
        }
        return n;
    }

    /**
     * Marks the rest of the chain as committed to prevent commits on premature
     * scan shutdown. Does not call detachNext/commit - just iterates and sets
     * the committed flag.
     */
    private void resetFullChain() {
        if (context.isRunning()) {
            LOG.warn("Logical error: resetFullChain() call on the running context");
            return;
        }
        MvScanCommitHandler n = next.get();
        while (n != null) {
            n.committed.set(true);
            n = n.next.get();
        }
    }

    private boolean isReady() {
        MvScanCommitHandler p = this;
        while (p != null) {
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
