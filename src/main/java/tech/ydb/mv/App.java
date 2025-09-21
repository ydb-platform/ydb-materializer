package tech.ydb.mv;

import tech.ydb.mv.mgt.MvBatchSettings;
import tech.ydb.mv.mgt.MvCoordinator;
import tech.ydb.mv.mgt.MvRunner;
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

    private volatile MvRunner runner = null;
    private volatile MvCoordinator coord = null;

    public App(YdbConnector conn, MvApi api) {
        this.conn = conn;
        this.api = api;
    }

    public static void main(String[] args) {
        if (args.length != 2 || MvConfig.parseMode(args[1]) == null) {
            System.out.println("USAGE: tech.ydb.mv.App job.xml CHECK|SQL|RUN");
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

    public void run(MvConfig.Mode mode) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> api.shutdown()));
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
            case LOCAL -> {
                LOG.info("Local service requested.");
                api.runDefaultHandlers();
            }
            case JOB -> {
                LOG.info("Distributed job service requested.");
                runJobService();
            }
        }
    }

    private void runJobService() {
        var batchSettings = new MvBatchSettings(api.getYdb().getConfig().getProperties());
        try (var theRunner = new MvRunner(api.getYdb(), api, batchSettings)) {
            runner = theRunner;
            try (var theCoord = MvCoordinator.newInstance(
                    api.getYdb(),
                    batchSettings,
                    runner.getRunnerId(),
                    api.getScheduler()
            )) {
                coord = theCoord;
                runner.start();
                coord.start();
                while (api.isRunning()) {
                    YdbMisc.sleep(200L);
                }
            } finally {
                coord = null;
            }
        } finally {
            runner = null;
        }
    }

}
