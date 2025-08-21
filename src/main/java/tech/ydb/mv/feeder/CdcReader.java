package tech.ydb.mv.feeder;

import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.CommitOffsetAcknowledgementEvent;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;
import tech.ydb.topic.settings.ReaderSettings;

/**
 *
 * @author zinal
 */
public class CdcReader implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CdcReader.class);

    private final AsyncReader reader;

    public CdcReader(TopicClient topicClient, MvHandler mh) {
        this.reader = buildReader(topicClient, mh);
    }

    @Override
    public void close() {
        reader.shutdown();
    }

    public void start() {
        reader.init();
    }

    private static AsyncReader buildReader(TopicClient topicClient, MvHandler mh) {
        ReaderSettings.Builder builder = ReaderSettings.newBuilder();
        builder.build();
        return null;
    }

    private class Handler extends AbstractReadEventHandler {
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
            for (Message msg: event.getMessages()) {
//                writer.addMessage(event.getPartitionSession().getPartitionId(), msg);
            }
        }
    }

}
