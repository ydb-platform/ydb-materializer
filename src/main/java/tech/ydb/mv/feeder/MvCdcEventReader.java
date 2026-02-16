package tech.ydb.mv.feeder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;

import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.metrics.MvMetrics;

/**
 *
 * @author zinal
 */
class MvCdcEventReader extends AbstractReadEventHandler {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcEventReader.class);

    private final MvCdcFeeder owner;
    private final MvSink sink;
    private final HashSet<Long> closedPartitions = new HashSet<>();

    MvCdcEventReader(MvCdcFeeder owner) {
        this.owner = owner;
        this.sink = owner.getSink();
    }

    public boolean isRunning() {
        return owner.isRunning();
    }

    @Override
    public void onStartPartitionSession(StartPartitionSessionEvent ev) {
        ev.confirm();
        boolean exists;
        synchronized (closedPartitions) {
            exists = closedPartitions.remove(ev.getPartitionSession().getPartitionId());
        }
        if (exists) {
            LOG.info("Feeder `{}` topic `{}` session {} for partition {} "
                    + "re-established with last committed offset {}",
                    owner.getName(), ev.getPartitionSession().getPath(),
                    ev.getPartitionSession().getId(), ev.getPartitionSession().getPartitionId(),
                    ev.getCommittedOffset());
        } else {
            LOG.debug("Feeder `{}` topic `{}` session {} for partition {} "
                    + "onStart with last committed offset {}",
                    owner.getName(), ev.getPartitionSession().getPath(),
                    ev.getPartitionSession().getId(), ev.getPartitionSession().getPartitionId(),
                    ev.getCommittedOffset());
        }
    }

    @Override
    public void onStopPartitionSession(StopPartitionSessionEvent ev) {
        LOG.debug("Feeder `{}` topic `{}` session {} onStop with last committed offset {}",
                owner.getName(), ev.getPartitionSession().getPath(),
                ev.getPartitionSession().getId(), ev.getCommittedOffset());
        ev.confirm();
    }

    @Override
    public void onPartitionSessionClosed(PartitionSessionClosedEvent ev) {
        LOG.debug("Feeder `{}` topic `{}` session {} onClosed",
                owner.getName(), ev.getPartitionSession().getPath(),
                ev.getPartitionSession().getId());
        synchronized (closedPartitions) {
            closedPartitions.add(ev.getPartitionSession().getPartitionId());
        }
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        String topicPath = event.getPartitionSession().getPath();
        MvMetrics.CdcScope scope = new MvMetrics.CdcScope(
                owner.getFeederName(),
                owner.getConsumerName(),
                topicPath);
        MvMetrics.recordCdcRead(scope, event.getMessages().size());

        MvCdcParser parser = owner.findParser(topicPath);
        if (parser == null) {
            LOG.warn("Feeder `{}` skipping {} message(s) for unhandled topic `{}`",
                    owner.getName(), event.getMessages().size(), topicPath);
            event.commit();
            return;
        }

        long parseStart = System.nanoTime();
        ArrayList<MvChangeRecord> records = new ArrayList<>(event.getMessages().size());
        for (Message m : event.getMessages()) {
            Instant tv = m.getCreatedAt();
            if (tv == null) {
                tv = m.getWrittenAt();
            }
            if (tv == null) {
                tv = Instant.now();
            }
            MvCdcParser.ParseResult result = parser.parse(m.getData(), tv);
            if (result.getRecord() != null) {
                records.add(result.getRecord());
            }
        }
        LOG.trace("Topic `{}` parsed input: {}", topicPath, records);

        MvMetrics.recordCdcParse(scope, parseStart, event.getMessages().size(), records.size());

        if (records.isEmpty()) {
            LOG.warn("Feeder `{}` skipping {} message(s) for topic `{}` - nothing to process",
                    owner.getName(), event.getMessages().size(), topicPath);
            event.commit();
            return;
        }

        long submitStart = System.nanoTime();
        try {
            sink.submit(records, new MvCdcCommitHandler(this, event, records.size()));
            MvMetrics.recordCdcSubmit(scope, submitStart, records.size());
        } catch (Exception ex) {
            // We should not throw from onMessages(), as it stops the CDC reader.
            LOG.error("Feeder `{}` for topic `{}` SUBMIT FAILED",
                    owner.getName(), topicPath, ex);
        }
    }

}
