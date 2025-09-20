package tech.ydb.mv.integration;

import java.util.HashMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvApi;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.support.YdbMisc;

/**
 * @author zinal
 */
public class ConcurrencyIntegrationTest extends AbstractIntegrationBase {

    private final HashMap<String, Integer> numSuccess = new HashMap<>();

    @BeforeAll
    public static void init() {
        prepareDb();
    }

    @AfterAll
    public static void cleanup() {
        clearDb();
    }

    @Test
    public void concurrencyIntegrationTest() {
        System.err.println("[AAA] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        cfg.getProperties().setProperty(MvConfig.CONF_COORD_TIMEOUT, "5");

        Thread t1 = new Thread(() -> handler(cfg, "handler1"));
        Thread t2 = new Thread(() -> handler(cfg, "handler2"));
        Thread t1dup = new Thread(() -> handler(cfg, "handler1"));

        t1.start();
        t2.start();
        YdbMisc.sleep(100L);
        t1dup.start();

        try {
            t1.join();
        } catch (InterruptedException ix) {
        }
        try {
            t2.join();
        } catch (InterruptedException ix) {
        }
        try {
            t1dup.join();
        } catch (InterruptedException ix) {
        }

        Assertions.assertNotNull(numSuccess.get("handler1"));
        Assertions.assertEquals(1, numSuccess.get("handler1").intValue());
        Assertions.assertNotNull(numSuccess.get("handler2"));
        Assertions.assertEquals(1, numSuccess.get("handler2").intValue());
    }

    private void handler(YdbConnector.Config cfg, String name) {
        try (YdbConnector conn = new YdbConnector(cfg); MvApi api = MvApi.newInstance(conn)) {
            api.applyDefaults(conn.getConfig().getProperties());

            System.err.println("[" + name + "] Checking context...");
            api.printIssues(System.out);
            Assertions.assertTrue(api.getMetadata().isValid());

            api.startHandler(name);
            reportSuccess(name);
            YdbMisc.sleep(10000L);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }

    private void reportSuccess(String name) {
        synchronized (numSuccess) {
            numSuccess.merge(name, 1, Integer::sum);
        }
    }
}
