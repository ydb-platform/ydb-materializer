package tech.ydb.mv.feeder;

import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.read.Message;
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
    private final Executor executor;
    private final AtomicReference<AsyncReader> reader = new AtomicReference<>();
    // topicPath -> input definition
    private final HashMap<String, MvInput> inputs = new HashMap<>();

    public MvCdcReader(MvHandlerController owner) {
        this.owner = owner;
        this.executor = Executors.newFixedThreadPool(
                owner.getSettings().getCdcReaderThreads(), new Factory());
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

    public MvInput getInput(String topicPath) {
        return inputs.get(topicPath);
    }

    public void fire(MvInput input, Message m) {

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
                .setEventHandler(new MvCdcEventReader(this))
                .setExecutor(executor)
                .build();
        return owner.getConnector().getTopicClient().createAsyncReader(builder.build(), rehs);
    }


    private static class Factory implements ThreadFactory {
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
