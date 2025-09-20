package tech.ydb.mv.feeder;

import java.util.concurrent.atomic.AtomicLong;

import tech.ydb.topic.read.events.DataReceivedEvent;

/**
 * CDC commit handler moves the topic offsets forward when the original messages
 * are fully processed.
 *
 * @author zinal
 */
class MvCdcCommitHandler implements MvCommitHandler {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcCommitHandler.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);

    private final long instance;
    private final DataReceivedEvent event;
    private volatile int counter;
    private volatile boolean committed;

    public MvCdcCommitHandler(DataReceivedEvent event) {
        this.instance = COUNTER.incrementAndGet();
        this.event = event;
        this.counter = event.getMessages().size();
        this.committed = false;
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

    @Override
    public synchronized void commit(int count) {
        if (committed || counter < 0) {
            return;
        }
        counter -= Math.min(count, counter);
        LOG.debug("instance {} commit {} -> {}", instance, count, counter);
        if (counter == 0) {
            committed = true;
            LOG.debug("instance {} commit APPLY", instance);
            event.commit().exceptionally((t) -> reportError(t));
        }
    }

    private Void reportError(Throwable t) {
        LOG.error("Failed to commit the CDC message pack", t);
        return null;
    }

    @Override
    public synchronized void reserve(int count) {
        if (count > 0 && !committed) {
            counter += count;
            LOG.debug("instance {} reserve {} -> {}", instance, count, counter);
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (int) (this.instance ^ (this.instance >>> 32));
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
        final MvCdcCommitHandler other = (MvCdcCommitHandler) obj;
        return this.instance == other.instance;
    }

    @Override
    public String toString() {
        return "MvCdcCommitHandler{" + instance + '}';
    }

}
