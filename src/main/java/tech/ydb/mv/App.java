package tech.ydb.mv;

import java.io.FileInputStream;
import java.util.Properties;

/**
 *
 * @author mzinal
 */
public class App {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        if (args.length != 2 && args.length != 3) {
            System.out.println("USAGE: tech.ydb.samples.exporter.App connection.xml job.xml [properties.xml]");
            System.exit(1);
        }
        try {
            YdbConnector.Config ycc = YdbConnector.Config.fromFile(args[0]);
        } catch(Exception ex) {
            LOG.error("Execution failed", ex);
        }
    }

}
