package tech.ydb.mv.integration;

import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

    @BeforeEach
    public void runBefore() {
        prepareDb();
    }

    @AfterEach
    public void runAfter() {
        clearDb();
    }

    @Test
    public void concurrencyIntegrationTest() {
        System.err.println("[CCC] Starting up...");
        var cfg = MvConfig.fromBytes(getConfigBytes());
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

    private void handler(MvConfig cfg, String name) {
        try (YdbConnector conn = new YdbConnector(cfg, true); MvApi api = MvApi.newInstance(conn)) {
            api.applyDefaults(conn.getConfig().getProperties());

            System.err.println("[CCC] Checking context for handler " + name);
            api.printIssues(System.out);
            Assertions.assertTrue(api.getMetadata().isValid());

            if (api.startHandler(name)) {
                reportSuccess(name);
            }
            YdbMisc.sleep(10_000L);
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
