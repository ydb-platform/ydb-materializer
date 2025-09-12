package tech.ydb.mv.integration;

import java.util.HashMap;
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

    private final HashMap<String, Integer> numSuccess = new HashMap<>();

    @Test
    public void concurrencyIntegrationTest() {
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
        Thread t1dup = new Thread(() -> handler(cfg, "handler1"));

        t1.start();
        t2.start();
        YdbMisc.sleep(50L);
        t1dup.start();

        try { t1.join(); } catch(InterruptedException ix) {}
        try { t2.join(); } catch(InterruptedException ix) {}
        try { t1dup.join(); } catch(InterruptedException ix) {}

        Assertions.assertNotNull(numSuccess.get("handler1"));
        Assertions.assertEquals(1, numSuccess.get("handler1").intValue());
        Assertions.assertNotNull(numSuccess.get("handler2"));
        Assertions.assertEquals(1, numSuccess.get("handler2").intValue());
}

    private void handler(YdbConnector.Config cfg, String name) {
        try ( YdbConnector conn = new YdbConnector(cfg);
                MvService wc = new MvService(conn) ) {
            wc.applyDefaults();

            System.err.println("[" + name + "] Checking context...");
            wc.printIssues();
            Assertions.assertTrue(wc.getMetadata().isValid());

            if ( wc.startHandler(name) ) {
                reportSuccess(name);
            }
            YdbMisc.sleep(5000L);
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
        }
    }

    private void reportSuccess(String name) {
        synchronized(numSuccess) {
            Integer v = numSuccess.get(name);
            if (v==null) {
                numSuccess.put(name, 1);
            } else {
                numSuccess.put(name, v + 1);
            }
        }
    }

}
