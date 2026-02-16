package tech.ydb.mv.feeder;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvInput;

/**
 * CDC message feeder for YDB Materializer.
 *
 * @author zinal
 */
public class MvCdcFeeder implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCdcFeeder.class);

    private final MvCdcAdapter adapter;
    private final YdbConnector ydb;
    private final MvSink sink;
    private final ExecutorService executor;
    private final AtomicReference<AsyncReader> reader = new AtomicReference<>();
    // topicPath -> parser definition
    private final HashMap<String, MvCdcParser> parsers = new HashMap<>();

    /**
     * Create a CDC feeder.
     *
     * @param adapter CDC adapter providing consumer configuration.
     * @param ydb YDB connector.
     * @param sink Sink receiving parsed change records.
     */
    public MvCdcFeeder(MvCdcAdapter adapter, YdbConnector ydb, MvSink sink) {
        this.adapter = adapter;
        this.ydb = ydb;
        this.sink = sink;
        this.executor = Executors.newFixedThreadPool(
                adapter.getCdcReaderThreads(), new ConfigureThreads(adapter));
        LOG.info("Started {} CDC reader threads for handler `{}`",
                adapter.getCdcReaderThreads(),
                adapter.getFeederName());
    }

    /**
     * Get feeder name.
     *
     * @return Feeder identifier used for logging/diagnostics.
     */
    public String getName() {
        return adapter.getFeederName();
    }

    /**
     * Get the consumer name configured for the CDC feeder.
     *
     * @return Consumer name.
     */
    public String getConsumerName() {
        return adapter.getConsumerName();
    }

    /**
     * Get the name for the handler (job) which runs the CDC feeder.
     *
     * @return Handler (e.g. job) name for the CDC feeder.
     */
    public String getFeederName() {
        return adapter.getFeederName();
    }

    public boolean isRunning() {
        return adapter.isRunning();
    }

    /**
     * Start CDC consumption (idempotent).
     */
    public synchronized void start() {
        if (reader.get() != null) {
            return;
        }
        if (!adapter.isRunning()) {
            return;
        }
        AsyncReader theReader = buildReader();
        if (theReader == null) {
            LOG.info("Empty topic list, refusing to start the CDC reader in feeder `{}`", adapter.getFeederName());
            return;
        }
        LOG.info("Activating the CDC reader for feeder `{}`", adapter.getFeederName());
        theReader.init();
        reader.set(theReader);
    }

    /**
     * Stop CDC consumption (idempotent).
     */
    public synchronized void stop() {
        if (adapter.isRunning()) {
            LOG.error("Ignoring an attempt to stop the CDC reader for feeder `{}` "
                    + "while controller is still running", adapter.getFeederName());
            return;
        }
        AsyncReader theReader = reader.getAndSet(null);
        if (theReader == null) {
            return;
        }
        LOG.info("Stopping the CDC reader for feeder `{}`", adapter.getFeederName());
        try {
            theReader.shutdown().get(10L, TimeUnit.SECONDS);
        } catch (Exception ex) {
            LOG.warn("CDC reader shutdown was not successful or timed out", ex);
        }
    }

    /**
     * Stop CDC reader and shutdown worker threads.
     */
    @Override
    public void close() {
        stop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                int total = executor.shutdownNow().size();
                LOG.error("Total of {} tasks have not completed "
                        + "within the timeout of 30 seconds.", total);
            }
        } catch (InterruptedException ix) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get sink used by this feeder.
     *
     * @return Sink instance.
     */
    public MvSink getSink() {
        return sink;
    }

    MvCdcParser findParser(String topicPath) {
        return parsers.get(topicPath);
    }

    private AsyncReader buildReader() {
        ReaderSettings.Builder builder = ReaderSettings.newBuilder()
                .setDecompressionExecutor(Runnable::run) // CDC doesn't use compression, skip thread switching
                .setMaxMemoryUsageBytes(200 * 1024 * 1024) // 200 Mb
                .setConsumerName(adapter.getConsumerName());
        int topicCount = 0;
        for (MvInput mi : sink.getInputs()) {
            String topicPath = ydb.fullCdcTopicName(mi.getTableName(), mi.getChangefeed());
            if (parsers.containsKey(topicPath)) {
                LOG.warn("Skipped duplicate topic: `{}` for `{}`",
                        topicPath, adapter.getFeederName());
                continue;
            }
            parsers.put(topicPath, new MvCdcParser(mi));
            builder.addTopic(TopicReadSettings.newBuilder()
                    .setPath(topicPath)
                    .build());
            LOG.debug("Topic `{}` reading configured for `{}` in consumer `{}`",
                    topicPath, adapter.getFeederName(), adapter.getConsumerName());
            ++topicCount;
        }
        if (topicCount == 0) {
            return null;
        }
        ReadEventHandlersSettings rehs = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new MvCdcEventReader(this))
                .setExecutor(executor)
                .build();
        return ydb.getTopicClient().createAsyncReader(builder.build(), rehs);
    }

    private static class ConfigureThreads implements ThreadFactory {

        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final MvCdcAdapter adapter;

        public ConfigureThreads(MvCdcAdapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                    "mv-cdc-worker-" + adapter.getFeederName()
                    + "-" + threadNumber.getAndIncrement(), 0);
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
