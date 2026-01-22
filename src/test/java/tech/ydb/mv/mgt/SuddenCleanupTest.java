package tech.ydb.mv.mgt;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.MvApi;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.support.YdbMisc;

/**
 *
 * @author mzinal
 */
public class SuddenCleanupTest extends MgmtTestBase {

    private final ArrayList<MvRunner> runners = new ArrayList<>();

    private ArrayList<MvRunner> copyRunners() {
        synchronized (runners) {
            return new ArrayList<>(runners);
        }
    }

    @BeforeAll
    public static void setup() {
        prepareMgtDb();
        runDdl(ydbConnector, CREATE_TABLES_BASE);
    }

    @AfterAll
    public static void cleanup() {
        clearMgtDb();
    }

    @Test
    public void testSuddenCleanup() {
        final int numThreads = 1;
        var pool = Executors.newFixedThreadPool(numThreads);
        for (int ix = 0; ix < numThreads; ++ix) {
            pool.submit(() -> workerThread());
        }

        pause(20000L);

        var activeRunners = copyRunners();
        while (!activeRunners.isEmpty()) {
            for (var runner : activeRunners) {
                runner.stop();
            }
            standardPause();
            activeRunners = copyRunners();
        }

        boolean isTerminated = false;
        pool.shutdown();
        try {
            isTerminated = pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException ix) {
        }
        Assertions.assertTrue(isTerminated, "Jobs did not shut down within 30 seconds");
    }

    public int workerThread() {
        System.out.println("Worker entry");
        try {
            YdbConnector.Config cfg = new YdbConnector.Config(getConfigProps(), null);
            try (YdbConnector conn = new YdbConnector(cfg)) {
                try (MvApi api = MvApi.newInstance(conn)) {
                    var batchSettings = new MvBatchSettings(api.getYdb().getConfig().getProperties());
                    try (var theRunner = new MvRunner(api.getYdb(), api, batchSettings)) {
                        try (var theCoord = MvCoordinator.newInstance(
                                api.getYdb(),
                                batchSettings,
                                theRunner.getRunnerId(),
                                api.getScheduler()
                        )) {
                            synchronized (runners) {
                                runners.add(theRunner);
                            }
                            theRunner.start();
                            theCoord.start();
                            while (theRunner.isRunning()) {
                                YdbMisc.sleep(200L);
                            }
                        } finally {
                            synchronized (runners) {
                                runners.remove(theRunner);
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.out.println("Worker exit - FAILURE");
            return -1;
        }
        System.out.println("Worker exit - SUCCESS");
        return 0;
    }

    @Override
    protected Properties getConfigProps() {
        Properties props = super.getConfigProps();
        for (var pair : getMgtProperties().entrySet()) {
            props.setProperty(pair.getKey().toString(), pair.getValue().toString());
        }
        props.remove(MvConfig.CONF_HANDLERS);
        return props;
    }
}
