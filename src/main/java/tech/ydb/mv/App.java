package tech.ydb.mv;

/**
 * Default application for YDB Materializer.
 * @author mzinal
 */
public class App {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        if (args.length != 2 || parseMode(args[1])==null) {
            System.out.println("USAGE: tech.ydb.mv.App job.xml CHECK|SQL|RUN");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
            try (Tool tool = new Tool(ycc)) {
                switch (parseMode(args[1])) {
                    case CHECK:
                        tool.check();
                        break;
                    case SQL:
                        tool.showSql();
                        break;
                    case RUN:
                        tool.run();
                        break;
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

    public static enum Mode {
        CHECK,
        SQL,
        RUN
    }

}
