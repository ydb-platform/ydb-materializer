package tech.ydb.mv.feeder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import tech.ydb.mv.MvController;
import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;

/**
 * CDC message reader for YDB Materializer.
 *
 * @author zinal
 */
public class MvCdcReader {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcReader.class);

    private final MvController owner;
    private final MvApplyManager applyManager;
    private final Executor executor;
    private final AtomicReference<AsyncReader> reader = new AtomicReference<>();
    // topicPath -> parser definition
    private final HashMap<String, MvCdcParser> parsers = new HashMap<>();

    public MvCdcReader(MvController owner) {
        this.owner = owner;
        this.applyManager = owner.getApplyManager();
        this.executor = Executors.newFixedThreadPool(
                owner.getSettings().getCdcReaderThreads(), new ConfigureThreads());
    }

    public synchronized void start() {
        if (reader.get() != null) {
            return;
        }
        LOG.info("Starting the CDC reader for handler {}", owner.getMetadata().getName());
        AsyncReader theReader = buildReader();
        theReader.init();
        reader.set(theReader);
    }

    public synchronized void stop() {
        AsyncReader theReader = reader.getAndSet(null);
        if (theReader!=null) {
            LOG.info("Stopping the CDC reader for handler {}", owner.getMetadata().getName());
            theReader.shutdown();
        }
    }

    public MvCdcParser getParser(String topicPath) {
        return parsers.get(topicPath);
    }

    public void fire(MvCdcParser parser, DataReceivedEvent event) {
        ArrayList<MvChangeRecord> records = new ArrayList<>(event.getMessages().size());
        for (Message m : event.getMessages()) {
            records.add(parser.parse(m.getData()));
        }
        applyManager.submit(records, new MvCdcCommitHandler(event));
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
            parsers.put(topicPath, new MvCdcParser(mi));
            builder.addTopic(TopicReadSettings.newBuilder()
                    .setPath(topicPath)
                    .build());
        }
        ReadEventHandlersSettings rehs = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new MvCdcEventReader(this))
                .setExecutor(executor)
                .build();
        return owner.getConnector().getTopicClient().createAsyncReader(builder.build(), rehs);
    }

    private static class ConfigureThreads implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                                  "ydb-cdc-worker-" + threadNumber.getAndIncrement(),
                                  0);
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
