package tech.ydb.mv.metrics;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Unit;

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

    public static synchronized void init(Config config, PrometheusRegistry provided) {
        if (config == null || !config.isEnabled()) {
            return;
        }
        if (ENABLED.get()) {
            return;
        }
        PrometheusRegistry registry = (provided == null)
                ? PrometheusRegistry.defaultRegistry
                : provided;
        metrics = new Metrics(registry);
        if (provided != null) {
            LOG.info("Prometheus metrics collector configured.");
            server = null;
        } else {
            // Provide JVM metrics
            JvmMetrics.builder().register(registry);
            // Start a dedicated HTTP collector instance
            try {
                server = HTTPServer.builder()
                        .hostname(config.getHost())
                        .port(config.getPort())
                        .buildAndStart();
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
        var current = server;
        server = null;
        metrics = null;
        if (current != null) {
            current.close();
        }
        ENABLED.set(false);
    }

    public static void recordHandlerState(String handler,
            boolean running, boolean locked) {
        var m = metrics;
        if (handler == null || m == null) {
            return;
        }
        String[] labels = {handler};
        m.jobActive.labelValues(labels).set(running ? 1 : 0);
        m.jobLocked.labelValues(labels).set(running && locked ? 1 : 0);
    }

    public static void recordHandlerStats(String handler,
            int nthreads, int queueSize, int queueLimit) {
        var m = metrics;
        if (handler == null || m == null) {
            return;
        }
        String[] labels = {handler};
        m.jobThreads.labelValues(labels).set(nthreads);
        m.jobQueueSize.labelValues(labels).set(queueSize);
        m.jobQueueLimit.labelValues(labels).set(queueLimit);
    }

    public static void recordQueueWait(String handler) {
        var m = metrics;
        if (handler == null || m == null) {
            return;
        }
        String[] labels = {handler};
        m.jobQueueWait.labelValues(labels).inc();
    }

    public static void recordCdcRead(CdcScope scope, int count) {
        var m = metrics;
        if (scope == null || m == null || count <= 0) {
            return;
        }
        String[] labels = getCdcLabels(scope);
        m.cdcRead.labelValues(labels).inc(count);
    }

    public static void recordCdcParse(CdcScope scope, long startNs, int input, int output) {
        var m = metrics;
        if (scope == null || m == null || input <= 0) {
            return;
        }
        long durationNs = System.nanoTime() - startNs;
        String[] labels = getCdcLabels(scope);
        m.cdcParseTime.labelValues(labels).observe(toSeconds(durationNs));
        if (output < input) {
            m.cdcParseErrors.labelValues(labels).inc(input - output);
        }
    }

    public static void recordCdcSubmit(CdcScope scope, long startNs, int count) {
        var m = metrics;
        if (scope == null || m == null) {
            return;
        }
        long durationNs = System.nanoTime() - startNs;
        String[] labels = getCdcLabels(scope);
        m.cdcSubmitTime.labelValues(labels).observe(toSeconds(durationNs));
        if (count > 0) {
            m.cdcSubmitted.labelValues(labels).inc(count);
        }
    }

    public static void recordProcessedSuccess(ActionScope scope, String action, long startNs, int count) {
        var m = metrics;
        if (scope == null || m == null || count <= 0) {
            return;
        }
        String[] labels = getActionLabels(scope, action);
        recordProcessingTime(m, labels, startNs);
        m.processedRecords.labelValues(labels).inc(count);
    }

    public static void recordProcessedError(ActionScope scope, String action, long startNs, int count) {
        var m = metrics;
        if (scope == null || m == null || count <= 0) {
            return;
        }
        String[] labels = getActionLabels(scope, action);
        recordProcessingTime(m, labels, startNs);
        m.processingErrors.labelValues(labels).inc(count);
    }

    private static void recordProcessingTime(Metrics m, String labels[], long startNs) {
        long durationNs = System.nanoTime() - startNs;
        m.processingTime.labelValues(labels).observe(toSeconds(durationNs));
    }

    public static void recordSqlTime(ActionScope scope, String action, long startNs) {
        var m = metrics;
        if (scope == null || m == null) {
            return;
        }
        long durationNs = System.nanoTime() - startNs;
        String[] labels = getActionLabels(scope, action);
        m.sqlTime.labelValues(labels).observe(toSeconds(durationNs));
    }

    public static void recordScanSubmit(ScanScope scope, int count) {
        var m = metrics;
        if (scope == null || m == null || count <= 0) {
            return;
        }
        String[] labels = getScanLabels(scope);
        m.scanRecords.labelValues(labels).inc(count);
    }

    public static void recordScanDelay(ScanScope scope, long delayMillis) {
        var m = metrics;
        if (scope == null || m == null || delayMillis <= 0L) {
            return;
        }
        String[] labels = getScanLabels(scope);
        m.scanDelays.labelValues(labels).inc(delayMillis);
    }

    private static String[] getActionLabels(ActionScope scope, String action) {
        String[] labels = {
            safeLabel(scope.type()),
            safeLabel(scope.handler()),
            safeLabel(scope.target()),
            safeLabel(scope.alias()),
            safeLabel(scope.source()),
            safeLabel(action)
        };
        return labels;
    }

    private static String[] getCdcLabels(CdcScope scope) {
        String[] labels = {
            safeLabel(scope.handler()),
            safeLabel(scope.consumer()),
            safeLabel(scope.topic())
        };
        return labels;
    }

    private static String[] getScanLabels(ScanScope scope) {
        String[] labels = {
            safeLabel(scope.handler()),
            safeLabel(scope.target()),
            safeLabel(scope.alias())
        };
        return labels;
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
                getAlias(target), null);
    }

    public static ActionScope scopeForActionGrab(MvHandler handler, MvViewExpr target, MvJoinSource src) {
        return new ActionScope("grabKeys", handler.getName(), target.getName(),
                getAlias(target), src.getTableName());
    }

    public static ActionScope scopeForActionTransform(MvHandler handler,
            MvViewExpr target, MvJoinSource src) {
        return new ActionScope("transform", handler.getName(), target.getName(),
                getAlias(target), src.getTableName());
    }

    public static ActionScope scopeForActionSync(MvHandler handler, MvViewExpr target) {
        return new ActionScope("sync", handler.getName(), target.getName(),
                getAlias(target), target.getTopMostSource().getTableName());
    }

    private static class Metrics {

        final Counter cdcRead;
        final Counter cdcParseErrors;
        final Counter cdcSubmitted;
        final Histogram cdcParseTime;
        final Histogram cdcSubmitTime;

        final Counter scanRecords;
        final Counter scanDelays;
        final Counter processedRecords;
        final Counter processingErrors;
        final Histogram processingTime;
        final Histogram sqlTime;

        final Gauge jobActive;
        final Gauge jobLocked;
        final Gauge jobThreads;
        final Gauge jobQueueSize;
        final Gauge jobQueueLimit;
        final Counter jobQueueWait;

        public Metrics(PrometheusRegistry registry) {
            double[] timingBounds = {
                0.05, 0.1, 0.15, 0.2, 0.25, 0.5, 0.75, 1.0, 2.0, 3.0, 4.0, 5.0,
                6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 20.0,
                25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 75.0, 100.0, 150.0, 200.0,
                250.0, 300.0, 350.0, 400.0, 500.0, 1000.0
            };

            String[] cdcLabels = {"handler", "consumer", "topic"};
            cdcRead = Counter.builder()
                    .name("ydbmv_cdc_records_read")
                    .help("CDC records read from topics")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcParseErrors = Counter.builder()
                    .name("ydbmv_cdc_parse_errors")
                    .help("CDC parsing errors")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcSubmitted = Counter.builder()
                    .name("ydbmv_cdc_records_submitted")
                    .help("CDC records submitted for processing")
                    .labelNames(cdcLabels)
                    .register(registry);
            cdcParseTime = Histogram.builder()
                    .name("ydbmv_cdc_parse_seconds")
                    .help("CDC message parsing time histogram")
                    .labelNames(cdcLabels)
                    .classicUpperBounds(timingBounds)
                    .unit(Unit.SECONDS)
                    .register(registry);
            cdcSubmitTime = Histogram.builder()
                    .name("ydbmv_cdc_submit_seconds")
                    .help("CDC message submission time histogram")
                    .labelNames(cdcLabels)
                    .classicUpperBounds(timingBounds)
                    .unit(Unit.SECONDS)
                    .register(registry);

            String[] scanLabels = {"handler", "target", "alias"};
            scanRecords = Counter.builder()
                    .name("ydbmv_scan_records_submitted")
                    .help("Records submitted by scan")
                    .labelNames(scanLabels)
                    .register(registry);
            scanDelays = Counter.builder()
                    .name("ydbmv_scan_delay_millis")
                    .help("Total milliseconds of scan delays due to rate limiter")
                    .labelNames(scanLabels)
                    .register(registry);

            String[] procLabels = {"type", "handler", "target", "alias", "source", "action"};
            processedRecords = Counter.builder()
                    .name("ydbmv_processing_records")
                    .help("Records processed per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            processingErrors = Counter.builder()
                    .name("ydbmv_processing_errors")
                    .help("Processing errors per action and target")
                    .labelNames(procLabels)
                    .register(registry);
            processingTime = Histogram.builder()
                    .name("ydbmv_processing_seconds")
                    .help("Processing time histogram per action and target")
                    .labelNames(procLabels)
                    .classicUpperBounds(timingBounds)
                    .unit(Unit.SECONDS)
                    .register(registry);
            sqlTime = Histogram.builder()
                    .name("ydbmv_sql_seconds")
                    .help("SQL execution time histogram per action and target")
                    .labelNames(procLabels)
                    .classicUpperBounds(timingBounds)
                    .unit(Unit.SECONDS)
                    .register(registry);

            String[] jobLabels = {"handler"};
            jobActive = Gauge.builder()
                    .name("ydbmv_handler_active")
                    .help("Reports the number of active handlers (jobs)")
                    .labelNames(jobLabels)
                    .register(registry);
            jobLocked = Gauge.builder()
                    .name("ydbmv_handler_locked")
                    .help("Reports the number of locked handlers (jobs)")
                    .labelNames(jobLabels)
                    .register(registry);
            jobThreads = Gauge.builder()
                    .name("ydbmv_handler_threads")
                    .help("Reports the number of active threads per handler")
                    .labelNames(jobLabels)
                    .register(registry);
            jobQueueSize = Gauge.builder()
                    .name("ydbmv_handler_queue_size")
                    .help("Reports the size of input queue per handler")
                    .labelNames(jobLabels)
                    .register(registry);
            jobQueueLimit = Gauge.builder()
                    .name("ydbmv_handler_queue_limit")
                    .help("Reports the limit on input queue size per handler")
                    .labelNames(jobLabels)
                    .register(registry);
            jobQueueWait = Counter.builder()
                    .name("ydbmv_handler_queue_wait")
                    .help("Reports the number of waits on input queue per handler")
                    .labelNames(jobLabels)
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
            String source) {

    }

    public record CdcScope(
            String handler,
            String consumer,
            String topic) {

    }

    public record ScanScope(
            String handler,
            String target,
            String alias) {

    }

}
