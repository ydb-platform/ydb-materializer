package tech.ydb.mv.feeder;

import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;

/**
 * CDC message feeder for YDB Materializer.
 *
 * @author zinal
 */
public class MvCdcFeeder {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcFeeder.class);

    private final MvJobContext context;
    private final MvCdcSink sink;
    private final Executor executor;
    private final AtomicReference<AsyncReader> reader = new AtomicReference<>();
    // topicPath -> parser definition
    private final HashMap<String, MvCdcParser> parsers = new HashMap<>();

    public MvCdcFeeder(MvJobContext context, MvCdcSink sink) {
        this.context = context;
        this.sink = sink;
        this.executor = Executors.newFixedThreadPool(
                context.getSettings().getCdcReaderThreads(), new ConfigureThreads());
        LOG.info("Started {} CDC reader threads for handler {}",
                context.getSettings().getCdcReaderThreads(),
                context.getMetadata().getName());
    }

    public synchronized void start() {
        if (reader.get() != null) {
            return;
        }
        if (! context.isRunning()) {
            return;
        }
        LOG.info("Activating the CDC reader for handler {}", context.getMetadata().getName());
        AsyncReader theReader = buildReader();
        theReader.init();
        reader.set(theReader);
    }

    public synchronized void stop() {
        if (context.isRunning()) {
            LOG.error("Ignoring an attempt to stop the CDC reader for handler {} "
                    + "while controller is still running", context.getMetadata().getName());
            return;
        }
        AsyncReader theReader = reader.getAndSet(null);
        if (theReader!=null) {
            LOG.info("Stopping the CDC reader for handler {}", context.getMetadata().getName());
            theReader.shutdown();
        }
    }

    public MvCdcSink getSink() {
        return sink;
    }

    MvCdcParser findParser(String topicPath) {
        return parsers.get(topicPath);
    }

    private AsyncReader buildReader() {
        MvHandler handler = context.getMetadata();
        ReaderSettings.Builder builder = ReaderSettings.newBuilder()
                .setDecompressionExecutor(Runnable::run)   // CDC doesn't use compression, skip thread switching
                .setMaxMemoryUsageBytes(200 * 1024 * 1024) // 200 Mb
                .setConsumerName(handler.getConsumerNameAlways());
        for (MvInput mi : sink.getInputs()) {
            String topicPath = context.getYdb()
                    .fullCdcTopicName(mi.getTableName(), mi.getChangefeed());
            parsers.put(topicPath, new MvCdcParser(mi));
            builder.addTopic(TopicReadSettings.newBuilder()
                    .setPath(topicPath)
                    .build());
        }
        ReadEventHandlersSettings rehs = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new MvCdcEventReader(this))
                .setExecutor(executor)
                .build();
        return context.getYdb().getTopicClient().createAsyncReader(builder.build(), rehs);
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
