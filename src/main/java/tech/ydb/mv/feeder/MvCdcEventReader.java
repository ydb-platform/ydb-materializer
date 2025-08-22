package tech.ydb.mv.feeder;

import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;

import tech.ydb.mv.model.MvInput;

/**
 *
 * @author zinal
 */
class MvCdcEventReader extends AbstractReadEventHandler {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcEventReader.class);

    private final MvCdcReader owner;

    MvCdcEventReader(MvCdcReader owner) {
        this.owner = owner;
    }

    @Override
    public void onStartPartitionSession(StartPartitionSessionEvent ev) {
        LOG.info("Topic[{}] session {} onStart with last committed offset {}",
                ev.getPartitionSession().getPath(), ev.getPartitionSession().getId(), ev.getCommittedOffset());
        ev.confirm();
    }

    @Override
    public void onStopPartitionSession(StopPartitionSessionEvent ev) {
        LOG.info("Topic[{}] session {} onStop with last committed offset {}",
                ev.getPartitionSession().getPath(), ev.getPartitionSession().getId(), ev.getCommittedOffset());
        ev.confirm();
    }

    @Override
    public void onPartitionSessionClosed(PartitionSessionClosedEvent ev) {
        LOG.info("Topic[{}] session {} onClosed",
                ev.getPartitionSession().getPath(), ev.getPartitionSession().getId());
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        String topicPath = event.getPartitionSession().getPath();
        MvInput input = owner.getInput(topicPath);
        if (input == null) {
            LOG.warn("Skipping {} message(s) for unhandled topic {}",
                    event.getMessages().size(), topicPath);
            event.commit();
        } else {
            for (Message m : event.getMessages()) {
                owner.fire(input, m);
            }
        }
    }

}
