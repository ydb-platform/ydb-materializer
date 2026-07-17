package tech.ydb.mv.apply;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe counters for the apply queue size and its batch subset.
 *
 * @author zinal
 */
final class MvApplyQueueCounters {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(MvApplyQueueCounters.class);

    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicInteger batchQueueSize = new AtomicInteger(0);

    int getQueueSize() {
        return queueSize.get();
    }

    int getBatchQueueSize() {
        return batchQueueSize.get();
    }

    int increment(boolean batch) {
        if (batch) {
            batchQueueSize.incrementAndGet();
        }
        return queueSize.incrementAndGet();
    }

    int decrement(int count, int batchCount) {
        if (batchCount > 0) {
            int batchTemp = batchQueueSize.addAndGet(-1 * batchCount);
            if (batchTemp < 0) {
                LOG.error("Batch queue size below zero: {}", batchTemp);
                batchQueueSize.addAndGet(-1 * batchTemp);
            }
        }
        int temp = queueSize.addAndGet(-1 * count);
        if (temp < 0) {
            LOG.error("Queue size below zero: {}", temp);
            return queueSize.addAndGet(-1 * temp);
        }
        return temp;
    }

}
