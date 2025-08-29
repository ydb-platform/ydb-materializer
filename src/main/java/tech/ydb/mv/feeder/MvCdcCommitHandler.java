package tech.ydb.mv.feeder;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import tech.ydb.mv.apply.MvCommitHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

/**
 *
 * @author zinal
 */
class MvCdcCommitHandler implements MvCommitHandler {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcCommitHandler.class);
    private static final AtomicLong COUNTER = new AtomicLong(0L);

    private final long instance;
    private final DataReceivedEvent event;
    private final AtomicInteger counter;

    public MvCdcCommitHandler(DataReceivedEvent event) {
        this.instance = COUNTER.incrementAndGet();
        this.event = event;
        this.counter = new AtomicInteger(event.getMessages().size());
    }

    @Override
    public void apply(int count) {
        if (counter.addAndGet(-1 * count) <= 0) {
            try {
                event.commit().join();
            } catch (Exception ex) {
                LOG.warn("Failed to commit the CDC message pack", ex);
            }
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

}
