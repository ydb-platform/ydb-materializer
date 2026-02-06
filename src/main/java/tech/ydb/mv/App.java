package tech.ydb.mv;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.mgt.MvBatchSettings;
import tech.ydb.mv.mgt.MvCoordinator;
import tech.ydb.mv.mgt.MvRunner;
import tech.ydb.mv.metrics.MvMetrics;
import tech.ydb.mv.support.YdbMisc;

/**
 * Default application for YDB Materializer.
 *
 * @author zinal
 */
public class App {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    private final YdbConnector conn;
    private final MvApi api;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Create the application instance.
     *
     * @param conn YDB connection
     * @param api Materializer service API reference
     */
    public App(YdbConnector conn, MvApi api) {
        this.conn = conn;
        this.api = api;
    }

    /**
     * Application entry point.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        if (args.length != 2 || MvConfig.parseMode(args[1]) == null) {
            System.out.println("USAGE: tech.ydb.mv.App job.xml " + MvConfig.getAllModes());
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            LOG.info("Starting the YDB Materializer...");
            try (YdbConnector conn = new YdbConnector(ycc)) {
                LOG.info("Database connection established.");
                try (MvApi api = MvApi.newInstance(conn)) {
                    new App(conn, api).run(MvConfig.parseMode(args[1]));
                }
            }
            LOG.info("Completed successfully.");
        } catch (Exception ex) {
            LOG.error("Execution failed", ex);
            System.exit(1);
        }
    }

    private void initMetrics() {
        var config = new MvMetrics.Config();
        config.setEnabled(conn.getProperty(MvConfig.CONF_METRICS_ENABLED, false));
        if (config.isEnabled()) {
            config.setHost(conn.getProperty(MvConfig.CONF_METRICS_HOST, "0.0.0.0"));
            config.setPort(conn.getProperty(MvConfig.CONF_METRICS_PORT, 7311));
        }
        MvMetrics.init(config);
    }

    /**
     * Run the configured application in the mode specified.
     *
     * @param mode The mode to be used for running the application
     */
    public void run(MvConfig.Mode mode) {
        api.applyDefaults(conn.getConfig().getProperties());
        switch (mode) {
            case CHECK -> {
                LOG.info("Issues output requested.");
                api.printIssues(System.out);
            }
            case SQL -> {
                LOG.info("SQL output requested.");
                api.printSql(System.out);
            }
            case STREAMS -> {
                LOG.info("CDC streams generation requested.");
                api.generateStreams(true, new PrintStream(new ByteArrayOutputStream()));
            }
            case LOCAL -> {
                LOG.info("Local service requested.");
                initMetrics();
                setupHandler(() -> api.shutdown());
                try {
                    api.runDefaultHandlers();
                } finally {
                    api.shutdown();
                    MvMetrics.shutdown();
                }
            }
            case JOB -> {
                LOG.info("Distributed job service requested.");
                initMetrics();
                try {
                    runJobService();
                } finally {
                    MvMetrics.shutdown();
                }
            }
        }
    }

    private void runJobService() {
        var batchSettings = new MvBatchSettings(api.getYdb().getConfig().getProperties());
        try (var theRunner = new MvRunner(api.getYdb(), api, batchSettings)) {
            try (var theCoord = MvCoordinator.newInstance(
                    api.getYdb(),
                    batchSettings,
                    theRunner.getRunnerId(),
                    api.getScheduler()
            )) {
                theRunner.start();
                theCoord.start();
                running.set(true);
                setupHandler(() -> running.set(false));
                while (running.get() && theRunner.isRunning()) {
                    YdbMisc.sleep(200L);
                }
            }
        } finally {
            MvMetrics.shutdown();
            // YDB connection should not be closed here,
            // as it is managed on the upper level.
        }
    }

    @SuppressWarnings("sunapi")
    private void setupHandler(Runnable handler) {
        sun.misc.Signal.handle(new sun.misc.Signal("TERM"), (signal) -> handler.run());
        sun.misc.Signal.handle(new sun.misc.Signal("INT"), (signal) -> handler.run());
    }
}
