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

    public static final String DEF_FILE = "mv.sql";
    public static final String DEF_TABLE = "mv/statements";

    public static void main(String[] args) {
        if (args.length != 2 || parseMode(args[1])==null) {
            System.out.println("USAGE: tech.ydb.mv.App job.xml CHECK|SQL|RUN");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            try (YdbConnector conn = new YdbConnector(ycc)) {
                MvService wc = new MvService(conn);
                try {
                    switch (parseMode(args[1])) {
                        case CHECK:
                            wc.printIssues();
                            break;
                        case SQL:
                            wc.printSql();
                            break;
                        case RUN:
                            wc.runHandlers();
                            break;
                    }
                } finally {
                    wc.shutdown();
                }
            }
        } catch(Exception ex) {
            LOG.error("Execution failed", ex);
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
