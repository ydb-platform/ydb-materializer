package tech.ydb.mv;

/**
 * Default application for YDB Materializer.
 *
 * @author zinal
 */
public class App {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        if (args.length != 2 || MvConfig.parseMode(args[1])==null) {
            System.out.println("USAGE: tech.ydb.mv.App job.xml CHECK|SQL|RUN");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            LOG.info("Starting the YDB Materializer...");
            try (YdbConnector conn = new YdbConnector(ycc)) {
                LOG.info("Database connection established.");
                try (MvApi api = MvApi.newInstance(conn)) {
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> api.shutdown()));
                    api.applyDefaults(conn.getConfig().getProperties());
                    switch (MvConfig.parseMode(args[1])) {
                        case CHECK:
                            LOG.info("Issues output requested.");
                            api.printIssues(System.out);
                            break;
                        case SQL:
                            LOG.info("SQL output requested.");
                            api.printSql(System.out);
                            break;
                        case RUN:
                            LOG.info("Handlers service requested.");
                            api.runDefaultHandlers();
                            break;
                    }
                }
            }
            LOG.info("Completed successfully.");
        } catch(Exception ex) {
            LOG.error("Execution failed", ex);
            System.exit(1);
        }
    }

}
