package tech.ydb.mv.feeder;

import java.time.Instant;
import java.util.ArrayList;

import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.model.MvChangeRecord;

/**
 *
 * @author zinal
 */
class MvCdcEventReader extends AbstractReadEventHandler {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcEventReader.class);

    private final MvCdcFeeder owner;
    private final MvCdcSink sink;

    MvCdcEventReader(MvCdcFeeder owner) {
        this.owner = owner;
        this.sink = owner.getSink();
    }

    @Override
    public void onStartPartitionSession(StartPartitionSessionEvent ev) {
        LOG.info("Topic `{}` session {} for partition {} onStart with last committed offset {}",
                ev.getPartitionSession().getPath(), ev.getPartitionSession().getId(),
                ev.getPartitionSession().getPartitionId(), ev.getCommittedOffset());
        ev.confirm();
    }

    @Override
    public void onStopPartitionSession(StopPartitionSessionEvent ev) {
        LOG.info("Topic `{}` session {} onStop with last committed offset {}",
                ev.getPartitionSession().getPath(), ev.getPartitionSession().getId(), ev.getCommittedOffset());
        ev.confirm();
    }

    @Override
    public void onPartitionSessionClosed(PartitionSessionClosedEvent ev) {
        LOG.info("Topic `{}` session {} onClosed",
                ev.getPartitionSession().getPath(), ev.getPartitionSession().getId());
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        String topicPath = event.getPartitionSession().getPath();
        MvCdcParser parser = owner.findParser(topicPath);
        if (parser == null) {
            LOG.warn("Skipping {} message(s) for unhandled topic {}",
                    event.getMessages().size(), topicPath);
            event.commit();
            return;
        }
        ArrayList<MvChangeRecord> records = new ArrayList<>(event.getMessages().size());
        for (Message m : event.getMessages()) {
            Instant tv = m.getCreatedAt();
            if (tv == null) {
                tv = m.getWrittenAt();
            }
            if (tv == null) {
                tv = Instant.now();
            }
            MvChangeRecord cr = parser.parse(m.getData(), tv);
            if (cr != null) {
                records.add(cr);
            }
        }
        sink.submit(records, new MvCdcCommitHandler(event));
    }

}
