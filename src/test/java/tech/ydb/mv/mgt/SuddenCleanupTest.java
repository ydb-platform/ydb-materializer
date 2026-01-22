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
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

/**
 *
 * @author mzinal
 */
public class SuddenCleanupTest extends MgmtTestBase {

    private final ArrayList<WorkerInfo> workers = new ArrayList<>();

    private ArrayList<WorkerInfo> copyWorkers() {
        synchronized (workers) {
            return new ArrayList<>(workers);
        }
    }

    private WorkerInfo findCoordinator() {
        synchronized (workers) {
            for (var wi : workers) {
                if (wi.coordinator.isLeader()) {
                    return wi;
                }
            }
        }
        return null;
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
        final int numThreads = 3;
        var pool = Executors.newFixedThreadPool(numThreads);
        for (int ix = 0; ix < numThreads; ++ix) {
            pool.submit(() -> workerThread());
        }

        pause(3000L);

        WorkerInfo wiCoord = findCoordinator();
        Assertions.assertNotNull(wiCoord);
        System.out.println("Achtung! Sudden cleanup for coordinator's runner: " + wiCoord.runner.getRunnerId());
        ydbConnector.sqlWrite("DECLARE $runner_id AS Text; "
                + "UPDATE `test1/mv_runners` "
                + "SET updated_at=Timestamp('2021-01-01T00:00:00Z') "
                + "WHERE runner_id=$runner_id;",
                Params.of("$runner_id", PrimitiveValue.newText(wiCoord.runner.getRunnerId())));

        pause(10000L);
        pause(10000L);

        Assertions.assertTrue(wiCoord.coordinator.isLeader(), "Coordinator remains the leader");

        System.out.println("Shutting down...");
        var activeRunners = copyWorkers();
        while (!activeRunners.isEmpty()) {
            for (var runner : activeRunners) {
                runner.runner.stop();
            }
            standardPause();
            activeRunners = copyWorkers();
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
                        WorkerInfo wi = null;
                        try (var theCoord = MvCoordinator.newInstance(
                                api.getYdb(),
                                batchSettings,
                                theRunner.getRunnerId(),
                                api.getScheduler()
                        )) {
                            wi = new WorkerInfo(theRunner, theCoord);
                            synchronized (workers) {
                                workers.add(wi);
                            }
                            theRunner.start();
                            theCoord.start();
                            while (theRunner.isRunning()) {
                                YdbMisc.sleep(200L);
                            }
                        } finally {
                            synchronized (workers) {
                                workers.remove(wi);
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
        props.setProperty(MvBatchSettings.CONF_REPORT_PERIOD_MS, "7500");
        props.setProperty(MvBatchSettings.CONF_RUNNER_TIMEOUT_MS, "15000");
        return props;
    }

    static class WorkerInfo {

        final MvRunner runner;
        final MvCoordinator coordinator;

        public WorkerInfo(MvRunner runner, MvCoordinator coordinator) {
            this.runner = runner;
            this.coordinator = coordinator;
        }
    }
}
