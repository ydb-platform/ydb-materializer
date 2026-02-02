package tech.ydb.mv.feeder;

import java.time.Instant;
import java.util.ArrayList;

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

    MvCdcEventReader(MvCdcFeeder owner) {
        this.owner = owner;
        this.sink = owner.getSink();
    }

    @Override
    public void onStartPartitionSession(StartPartitionSessionEvent ev) {
        LOG.debug("Feeder `{}` topic `{}` session {} for partition {} "
                + "onStart with last committed offset {}",
                owner.getName(), ev.getPartitionSession().getPath(),
                ev.getPartitionSession().getId(), ev.getPartitionSession().getPartitionId(),
                ev.getCommittedOffset());
        ev.confirm();
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
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        String topicPath = event.getPartitionSession().getPath();
        String consumerName = owner.getConsumerName();
        MvMetrics.recordCdcRead(consumerName, topicPath, event.getMessages().size());
        MvCdcParser parser = owner.findParser(topicPath);
        if (parser == null) {
            LOG.warn("Feeder `{}` skipping {} message(s) for unhandled topic `{}`",
                    owner.getName(), event.getMessages().size(), topicPath);
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
            long parseStart = System.nanoTime();
            MvCdcParser.ParseResult result = parser.parse(m.getData(), tv);
            long parseTime = System.nanoTime() - parseStart;
            MvMetrics.recordCdcParse(consumerName, topicPath, parseTime, result.isError());
            if (result.getRecord() != null) {
                records.add(result.getRecord());
            }
        }
        LOG.trace("Topic `{}` parsed input: {}", topicPath, records);

        if (records.isEmpty()) {
            LOG.warn("Feeder `{}` skipping {} message(s) for topic `{}` - nothing to process",
                    owner.getName(), event.getMessages().size(), topicPath);
            event.commit();
            return;
        }

        long submitStart = System.nanoTime();
        try {
            sink.submit(records, new MvCdcCommitHandler(event, records.size()));
            long submitTime = System.nanoTime() - submitStart;
            MvMetrics.recordCdcSubmit(consumerName, topicPath, submitTime, records.size());
        } catch (Exception ex) {
            // We should not throw from onMessages(), as it stops the CDC reader.
            LOG.error("Feeder `{}` for topic `{}` SUBMIT FAILED",
                    owner.getName(), topicPath, ex);
        }
    }

}
