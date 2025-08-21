package tech.ydb.mv.feeder;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.events.PartitionSessionClosedEvent;
import tech.ydb.topic.read.events.StartPartitionSessionEvent;
import tech.ydb.topic.read.events.StopPartitionSessionEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import tech.ydb.mv.MvHandlerController;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;

/**
 * CDC message reader for YDB Materializer.
 *
 * @author zinal
 */
public class MvCdcReader {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcReader.class);

    private final MvHandlerController owner;
    private final MvCdcThreadPool cdcPool;
    private final AtomicReference<AsyncReader> reader = new AtomicReference<>();
    // topicPath -> input definition
    private final HashMap<String, MvInput> inputs = new HashMap<>();

    public MvCdcReader(MvHandlerController owner, MvCdcThreadPool cdcPool) {
        this.owner = owner;
        this.cdcPool = cdcPool;
    }

    public synchronized void start() {
        if (reader.get() != null) {
            return;
        }
        AsyncReader theReader = buildReader();
        theReader.init();
        reader.set(theReader);
    }

    public synchronized void stop() {
        AsyncReader theReader = reader.getAndSet(null);
        if (theReader!=null) {
            theReader.shutdown();
        }
    }

    private void fire(MvInput input, Message m) {

    }

    private AsyncReader buildReader() {
        MvHandler handler = owner.getMetadata();
        ReaderSettings.Builder builder = ReaderSettings.newBuilder()
                .setDecompressionExecutor(Runnable::run)   // CDC doesn't use compression, skip thread switching
                .setMaxMemoryUsageBytes(200 * 1024 * 1024) // 200 Mb
                .setConsumerName(handler.getConsumerNameAlways());
        for (MvInput mi : handler.getInputs().values()) {
            String topicPath = owner.getConnector()
                    .fullCdcTopicName(mi.getTableName(), mi.getChangeFeed());
            inputs.put(topicPath, mi);
            builder.addTopic(TopicReadSettings.newBuilder()
                    .setPath(topicPath)
                    .build());
        }
        ReadEventHandlersSettings rehs = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new Handler())
                .setExecutor(cdcPool.getExecutor())
                .build();
        return owner.getConnector().getTopicClient().createAsyncReader(builder.build(), rehs);
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
            String topicPath = event.getPartitionSession().getPath();
            MvInput input = inputs.get(topicPath);
            if (input==null) {
                LOG.warn("Received {} message(s) for unhandled topic {}",
                        event.getMessages().size(), topicPath);
                event.commit();
            } else {
                for (Message m : event.getMessages()) {
                    fire(input, m);
                }
            }
        }
    }

}
