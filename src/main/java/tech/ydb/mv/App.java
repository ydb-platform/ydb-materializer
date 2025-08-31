package tech.ydb.mv;

/**
 * Default application for YDB Materializer.
 * @author zinal
 */
public class App {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    public static final String CONF_INPUT_MODE = "job.input.mode";
    public static final String CONF_INPUT_FILE = "job.input.file";
    public static final String CONF_INPUT_TABLE = "job.input.table";
    public static final String CONF_HANDLERS = "job.handlers";
    public static final String CONF_COORD_PATH = "job.coordination.path";
    public static final String CONF_DEF_CDC_THREADS = "job.default.cdc.threads";
    public static final String CONF_DEF_APPLY_THREADS = "job.default.apply.threads";
    public static final String CONF_DEF_APPLY_QUEUE = "job.default.apply.queue";
    public static final String CONF_DEF_BATCH_SELECT = "job.default.batch.select";
    public static final String CONF_DEF_BATCH_UPSERT = "job.default.batch.upsert";

    public static final String DEF_FILE = "mv.sql";
    public static final String DEF_TABLE = "mv/statements";
    public static final String DEF_COORD_PATH = "mv/coordination";

    public static void main(String[] args) {
        if (args.length != 2 || parseMode(args[1])==null) {
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
                    switch (parseMode(args[1])) {
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

    public static Mode parseMode(String v) {
        if (v==null) {
            return null;
        }
        v = v.trim();
        for (Mode m : Mode.values()) {
            if (m.name().equalsIgnoreCase(v)) {
                return m;
            }
        }
        return null;
    }

    public static Input parseInput(String v) {
        if (v==null) {
            return null;
        }
        v = v.trim();
        for (Input i : Input.values()) {
            if (i.name().equalsIgnoreCase(v)) {
                return i;
            }
        }
        return null;
    }

    public static enum Mode {
        CHECK,
        SQL,
        RUN
    }

    public static enum Input {
        FILE,
        TABLE
    }

}
