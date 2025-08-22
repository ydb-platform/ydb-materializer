package tech.ydb.mv.feeder;

import java.util.concurrent.atomic.AtomicInteger;
import tech.ydb.mv.apply.MvCommitHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

/**
 *
 * @author zinal
 */
class MvCdcCommitHandler implements MvCommitHandler {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcCommitHandler.class);

    private final DataReceivedEvent event;
    private final AtomicInteger counter;

    public MvCdcCommitHandler(DataReceivedEvent event) {
        this.event = event;
        this.counter = new AtomicInteger(event.getMessages().size());
    }

    @Override
    public void apply(int count) {
        if (count <= 0) {
            return;
        }
        if (counter.addAndGet(-1 * count) <= 0) {
            try {
                event.commit().join();
            } catch (Exception ex) {
                LOG.warn("Failed to commit the CDC message pack", ex);
            }
        }
    }

}
