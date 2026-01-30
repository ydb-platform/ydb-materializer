package tech.ydb.mv.metrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import com.sun.net.httpserver.HttpServer;

import tech.ydb.mv.MvConfig;

/**
 * Prometheus metrics for YDB Materializer.
 *
 * @author zinal
 */
public final class MvMetrics {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvMetrics.class);
    private static final AtomicBoolean ENABLED = new AtomicBoolean(false);

    private static volatile HTTPServer server;
    private static volatile Metrics metrics;

    private MvMetrics() {
    }

    public static synchronized void init(Properties props) {
        if (props == null) {
            return;
        }
        if (ENABLED.get()) {
            return;
        }
        boolean enabled = Boolean.parseBoolean(
                props.getProperty(MvConfig.CONF_METRICS_ENABLED, "false"));
        if (!enabled) {
            return;
        }
        String host = props.getProperty(MvConfig.CONF_METRICS_HOST, "0.0.0.0");
        int port = Integer.parseInt(
                props.getProperty(MvConfig.CONF_METRICS_PORT, "9090"));
        CollectorRegistry registry = new CollectorRegistry();
        metrics = new Metrics(registry);
        try {
            InetSocketAddress address = new InetSocketAddress(host, port);
            HttpServer httpServer = HttpServer.create(address, 3);
            server = new HTTPServer(httpServer, registry, true);
        } catch (IOException ex) {
            LOG.error("Failed to start Prometheus metrics server on {}:{}",
                    host, port, ex);
            metrics = null;
            return;
        } catch (Exception ex) {
            LOG.error("Failed to initialize Prometheus metrics server", ex);
            metrics = null;
            return;
        }
        ENABLED.set(true);
        LOG.info("Prometheus metrics enabled on http://{}:{}", host, port);
    }

    @SuppressWarnings("deprecation")
    public static synchronized void shutdown() {
        HTTPServer current = server;
        server = null;
        metrics = null;
        if (current != null) {
            current.stop();
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

    public static void recordProcessedCount(String mvName,
                                            String sourceTable, String sourceAlias, int count) {
        Metrics m = metrics;
        if (m == null || count <= 0) {
            return;
        }
        m.processedRecords.labels(
                safeLabel(mvName),
                safeLabel(sourceAlias),
                safeLabel(sourceTable)
        ).inc(count);
    }

    public static void recordProcessingTime(String mvName,
                                            String sourceTable, String sourceAlias, long durationNs) {
        Metrics m = metrics;
        if (m == null) {
            return;
        }
        m.processingTime.labels(
                safeLabel(mvName),
                safeLabel(sourceAlias),
                safeLabel(sourceTable)
        ).observe(toSeconds(durationNs));
    }

    public static void recordProcessingError(String mvName,
                                             String sourceTable, String sourceAlias, int count) {
        Metrics m = metrics;
        if (m == null || count <= 0) {
            return;
        }
        m.processingErrors.labels(
                safeLabel(mvName),
                safeLabel(sourceAlias),
                safeLabel(sourceTable)
        ).inc(count);
    }

    public static void recordSqlTime(String mvName, String sourceTable,
                                     String sourceAlias, String operation, long durationNs) {
        Metrics m = metrics;
        if (m == null) {
            return;
        }
        m.sqlTime.labels(
                safeLabel(mvName),
                safeLabel(sourceAlias),
                safeLabel(sourceTable),
                safeLabel(operation)
        ).observe(toSeconds(durationNs));
    }

    private static String safeLabel(String value) {
        if (value == null) {
            return "unknown";
        }
        String v = value.trim();
        if (v.isEmpty()) {
            return "unknown";
        }
        return v;
    }

    private static double toSeconds(long durationNs) {
        if (durationNs <= 0L) {
            return 0D;
        }
        return durationNs / 1_000_000_000D;
    }


    private static class Metrics {

        final Counter cdcRead;
        final Counter cdcParseErrors;
        final Counter cdcSubmitted;
        final Histogram cdcParseTime;
        final Histogram cdcSubmitTime;

        final Counter processedRecords;
        final Counter processingErrors;
        final Histogram processingTime;
        final Histogram sqlTime;

        Metrics(CollectorRegistry registry) {
            cdcRead = Counter.build()
                    .name("ydbmv_cdc_records_read_total")
                    .help("CDC records read from topics")
                    .labelNames("consumer", "topic")
                    .register(registry);
            cdcParseErrors = Counter.build()
                    .name("ydbmv_cdc_parse_errors_total")
                    .help("CDC parsing errors")
                    .labelNames("consumer", "topic")
                    .register(registry);
            cdcSubmitted = Counter.build()
                    .name("ydbmv_cdc_records_submitted_total")
                    .help("CDC records submitted for processing")
                    .labelNames("consumer", "topic")
                    .register(registry);
            cdcParseTime = Histogram.build()
                    .name("ydbmv_cdc_parse_seconds")
                    .help("Time spent parsing CDC records")
                    .labelNames("consumer", "topic")
                    .register(registry);
            cdcSubmitTime = Histogram.build()
                    .name("ydbmv_cdc_submit_seconds")
                    .help("Time spent submitting CDC records for processing")
                    .labelNames("consumer", "topic")
                    .register(registry);
            processedRecords = Counter.build()
                    .name("ydbmv_mv_records_processed_total")
                    .help("Records processed per MV and source")
                    .labelNames("mv", "source_alias", "source_table")
                    .register(registry);
            processingErrors = Counter.build()
                    .name("ydbmv_mv_processing_errors_total")
                    .help("Processing errors per MV and source")
                    .labelNames("mv", "source_alias", "source_table")
                    .register(registry);
            processingTime = Histogram.build()
                    .name("ydbmv_mv_processing_seconds")
                    .help("Processing time per MV and source")
                    .labelNames("mv", "source_alias", "source_table")
                    .register(registry);
            sqlTime = Histogram.build()
                    .name("ydbmv_mv_sql_seconds")
                    .help("SQL execution time per MV and source")
                    .labelNames("mv", "source_alias", "source_table", "operation")
                    .register(registry);
        }
    }
}
