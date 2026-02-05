package tech.ydb.mv.metrics;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvViewExpr;

/**
 * Prometheus metrics for YDB Materializer.
 *
 * @author Kirill Kurdyukov
 */
public final class MvMetrics {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvMetrics.class);
    private static final AtomicBoolean ENABLED = new AtomicBoolean(false);

    private static volatile HTTPServer server;
    private static volatile Metrics metrics;

    private MvMetrics() {
    }

    public static void init(Config config) {
        init(config, null);
    }

    public static synchronized void init(Config config, CollectorRegistry provided) {
        if (config == null || !config.isEnabled()) {
            return;
        }
        if (ENABLED.get()) {
            return;
        }
        CollectorRegistry registry = (provided == null)
                ? new CollectorRegistry()
                : provided;
        metrics = new Metrics(registry);
        if (provided != null) {
            LOG.info("Prometheus metrics collector configured.");
            server = null;
        } else {
            // Provide JVM metrics
            JvmMetrics.builder().register();
            // Start a dedicated HTTP collector instance
            try {
                InetSocketAddress address = new InetSocketAddress(config.getHost(), config.getPort());
                server = new HTTPServer(address, registry);
            } catch (Exception ex) {
                LOG.error("Failed to start Prometheus metrics server on {}:{}",
                        config.getHost(), config.getPort(), ex);
                metrics = null;
                return;
            }
            LOG.info("Prometheus metrics enabled on http://{}:{}", config.getHost(), config.getPort());
        }
        ENABLED.set(true);
    }

    public static synchronized void shutdown() {
        HTTPServer current = server;
        server = null;
        metrics = null;
        if (current != null) {
            current.close();
        }
        ENABLED.set(false);
    }

    public static void recordCdcRead(String consumer, String topic, int count) {
        Metrics m = metrics;
        if (m == null || count <= 0) {
            return;
        }
        m.cdcRead.labels(safeLabel(consumer), safeLabel(topic)).inc(count);
    }

    public static void recordCdcParse(String consumer, String topic, long durationNs, boolean error) {
        Metrics m = metrics;
        if (m == null) {
            return;
        }
        m.cdcParseTime.labels(safeLabel(consumer), safeLabel(topic))
                .observe(toSeconds(durationNs));
        if (error) {
            m.cdcParseErrors.labels(safeLabel(consumer), safeLabel(topic)).inc();
        }
    }

    public static void recordCdcSubmit(String consumer, String topic, long durationNs, int count) {
        Metrics m = metrics;
        if (m == null) {
            return;
        }
        m.cdcSubmitTime.labels(safeLabel(consumer), safeLabel(topic))
                .observe(toSeconds(durationNs));
        if (count > 0) {
            m.cdcSubmitted.labels(safeLabel(consumer), safeLabel(topic)).inc(count);
        }
    }

    public static void recordProcessedSuccess(ActionScope scope, String action, long startNs, int count) {
        Metrics m = metrics;
        if (scope == null || m == null || count <= 0) {
            return;
        }
        String[] labels = getActionLabels(scope, action);
        recordProcessingTime(m, labels, startNs);
        m.processedRecords.labels(labels).inc(count);
    }

    public static void recordProcessedError(ActionScope scope, String action, long startNs, int count) {
        Metrics m = metrics;
        if (scope == null || m == null || count <= 0) {
            return;
        }
        String[] labels = getActionLabels(scope, action);
        recordProcessingTime(m, labels, startNs);
        m.processingErrors.labels(labels).inc(count);
    }

    private static void recordProcessingTime(Metrics m, String labels[], long startNs) {
        long durationNs = System.nanoTime() - startNs;
        m.processingTime.labels(labels).observe(toSeconds(durationNs));
        m.processingTimeTotal.labels(labels).inc(durationNs / 1000L);
    }

    private static String[] getActionLabels(ActionScope scope, String action) {
        String[] labels = {
            safeLabel(scope.type()),
            safeLabel(scope.handler()),
            safeLabel(scope.target()),
            safeLabel(scope.alias()),
            safeLabel(scope.source()),
            safeLabel(scope.item()),
            safeLabel(action)
        };
        return labels;
    }

    public static void recordSqlTime(ActionScope scope, String action, long startNs) {
        Metrics m = metrics;
        if (m == null) {
            return;
        }
        long durationNs = System.nanoTime() - startNs;
        String[] labels = getActionLabels(scope, action);
        m.sqlTime.labels(labels).observe(toSeconds(durationNs));
        m.sqlTimeTotal.labels(labels).inc(durationNs / 1000L);
    }

    private static String safeLabel(String value) {
        if (value == null) {
            return "";
        }
        String v = value.trim();
        if (v.isEmpty()) {
            return "";
        }
        return v;
    }

    private static double toSeconds(long durationNs) {
        if (durationNs <= 0L) {
            return 0D;
        }
        return durationNs / 1_000_000_000D;
    }

    private static String getAlias(MvViewExpr target) {
        String alias = target.getAlias();
        if (alias == null || alias.isBlank()) {
            alias = "default";
        }
        return alias;
    }

    public static ActionScope scopeForActionFilter(MvHandler handler, MvViewExpr target, MvViewExpr request) {
        return new ActionScope("filter", handler.getName(), target.getName(),
                getAlias(target), null, getAlias(request));
    }

    public static ActionScope scopeForActionGrab(MvHandler handler, MvViewExpr target, MvJoinSource src) {
        return new ActionScope("grabKeys", handler.getName(), target.getName(),
                getAlias(target), src.getTableName(), null);
    }

    public static ActionScope scopeForActionTransform(MvHandler handler, MvViewExpr target, MvJoinSource src) {
        return new ActionScope("transform", handler.getName(), target.getName(),
                getAlias(target), src.getTableName(), null);
    }

    public static ActionScope scopeForActionSync(MvHandler handler, MvViewExpr target) {
        return new ActionScope("sync", handler.getName(), target.getName(),
                getAlias(target), target.getView().getName(), null);
    }

    private static class Metrics {

        final Counter cdcRead;
        final Counter cdcParseErrors;
        final Counter cdcSubmitted;
        final Histogram cdcParseTime;
        final Histogram cdcSubmitTime;

        final Counter processedRecords;
        final Counter processingErrors;
        final Counter processingTimeTotal;
        final Histogram processingTime;
        final Counter sqlTimeTotal;
        final Histogram sqlTime;

        public Metrics(CollectorRegistry registry) {
            String[] cdcLabels = {"consumer", "topic"};
            cdcRead = Counter.build()
                    .name("ydbmv_cdc_records_read_total")
                    .help("CDC records read from topics")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcParseErrors = Counter.build()
                    .name("ydbmv_cdc_parse_errors_total")
                    .help("CDC parsing errors")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcSubmitted = Counter.build()
                    .name("ydbmv_cdc_records_submitted_total")
                    .help("CDC records submitted for processing")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcParseTime = Histogram.build()
                    .name("ydbmv_cdc_parse_seconds")
                    .help("Time spent parsing CDC records")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcSubmitTime = Histogram.build()
                    .name("ydbmv_cdc_submit_seconds")
                    .help("Time spent submitting CDC records for processing")
                    .labelNames(cdcLabels)
                    .register(registry);
            String[] procLabels = {"type", "handler", "target", "alias", "source", "item", "action"};
            processedRecords = Counter.build()
                    .name("ydbmv_mv_records_processed_total")
                    .help("Records processed per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            processingErrors = Counter.build()
                    .name("ydbmv_mv_processing_errors_total")
                    .help("Processing errors per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            processingTimeTotal = Counter.build()
                    .name("ydbmv_mv_processing_time_micros")
                    .help("Processing time in microseconds per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            processingTime = Histogram.build()
                    .name("ydbmv_mv_processing_seconds")
                    .help("Processing time per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            sqlTimeTotal = Counter.build()
                    .name("ydbmv_mv_sql_time_micros")
                    .help("SQL execution time in microseconds per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            sqlTime = Histogram.build()
                    .name("ydbmv_mv_sql_seconds")
                    .help("SQL execution time per action and target")
                    .labelNames(procLabels)
                    .register(registry);
        }
    }

    public static final class Config implements Serializable {

        private boolean enabled;
        private String host;
        private int port;

        public Config() {
            this.enabled = false;
        }

        public Config(boolean enabled) {
            this.enabled = enabled;
            this.host = "0.0.0.0";
            this.port = 7311;
        }

        public Config(boolean enabled, String host, int port) {
            this.enabled = enabled;
            this.host = host;
            this.port = port;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

    }

    public record ActionScope(
            String type,
            String handler,
            String target,
            String alias,
            String source,
            String item) {

    }

}
