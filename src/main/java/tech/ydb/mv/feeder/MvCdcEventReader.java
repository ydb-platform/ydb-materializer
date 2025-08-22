package tech.ydb.mv.feeder;

import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;

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
        MvCdcParser parser = owner.getParser(topicPath);
        if (parser == null) {
            LOG.warn("Skipping {} message(s) for unhandled topic {}",
                    event.getMessages().size(), topicPath);
            event.commit();
        } else {
            owner.fire(parser, event);
        }
    }

}
