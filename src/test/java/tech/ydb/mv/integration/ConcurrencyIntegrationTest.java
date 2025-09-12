package tech.ydb.mv.integration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.util.YdbMisc;

/**
 *
 * @author zinal
 */
public class ConcurrencyIntegrationTest extends AbstractIntegrationBase {

    @Test
    public void basicIntegrationTest() {
        // have to wait a bit here for YDB startup
        pause(1000L);
        // now the work
        System.err.println("[AAA] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            fillDatabase(conn);
        }

        System.err.println("[AAA] Preparation: completed.");

        Thread t1 = new Thread(() -> handler(cfg, "handler1"));
        Thread t2 = new Thread(() -> handler(cfg, "handler2"));

        t1.start();
        t2.start();

        try { t1.join(); } catch(InterruptedException ix) {}
        try { t2.join(); } catch(InterruptedException ix) {}
}

    private static void handler(YdbConnector.Config cfg, String name) {
        try ( YdbConnector conn = new YdbConnector(cfg);
                MvService wc = new MvService(conn) ) {
            wc.applyDefaults();

            System.err.println("[" + name + "] Checking context...");
            wc.printIssues();
            Assertions.assertTrue(wc.getMetadata().isValid());

            wc.startHandler(name);
            YdbMisc.sleep(5000L);
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
        }
    }

}
