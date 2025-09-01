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
                MvService wc = new MvService(conn);
                try {
                    switch (MvConfig.parseMode(args[1])) {
                        case CHECK:
                            LOG.info("Issues output requested.");
                            wc.printIssues();
                            break;
                        case SQL:
                            LOG.info("SQL output requested.");
                            wc.printSql();
                            break;
                        case RUN:
                            LOG.info("Handlers service requested.");
                            wc.runHandlers();
                            break;
                    }
                } finally {
                    wc.shutdown();
                }
            }
            LOG.info("Database connection closed.");
        } catch(Exception ex) {
            LOG.error("Execution failed", ex);
            System.exit(1);
        }
    }

}
