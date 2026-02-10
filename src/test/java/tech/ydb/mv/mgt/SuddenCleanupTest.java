package tech.ydb.mv.mgt;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

    private static final int NUM_THREADS = 3;
    private final ArrayList<WorkerInfo> workers = new ArrayList<>();
    private YdbConnector workerConn = null;

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

    private WorkerInfo findRegular() {
        synchronized (workers) {
            for (var wi : workers) {
                if (!wi.coordinator.isLeader()) {
                    return wi;
                }
            }
        }
        return null;
    }

    @BeforeEach
    public void setup() {
        prepareMgtDb();
        runDdl(ydbConnector, CREATE_TABLES_BASE);
        for (int i = 0; i < NUM_THREADS; ++i) {
            createDataTables(i);
            configureMv(i);
        }
        var cfg = new MvConfig(getConfigProps(), null);
        workerConn = new YdbConnector(cfg, true);
    }

    @AfterEach
    public void cleanup() {
        workerConn.close();
        for (int i = 0; i < NUM_THREADS; ++i) {
            dropDataTables(i);
        }
        runDdl(ydbConnector, DROP_TABLES_BASE);
        clearMgtDb();
    }

    @Test
    public void testSuddenCleanup() {
        var pool = Executors.newFixedThreadPool(NUM_THREADS);
        for (int ix = 0; ix < NUM_THREADS; ++ix) {
            pool.submit(() -> workerThread(workerConn));
        }

        pause(10000L);

        WorkerInfo wiCoord = findCoordinator();
        Assertions.assertNotNull(wiCoord);
        WorkerInfo wiReg = findRegular();
        Assertions.assertNotNull(wiReg);

        System.out.println("Achtung! Sudden cleanup for coordinator's runner: " + wiCoord.runner.getRunnerId());
        makeRunnerObsolete(wiCoord.runner.getRunnerId());

        pause(10000L);
        pause(10000L);

        Assertions.assertTrue(wiCoord.coordinator.isLeader(), "Initial leader remains the leader");

        System.out.println("Achtung! Sudden cleanup for regular runner: " + wiReg.runner.getRunnerId());
        makeRunnerObsolete(wiReg.runner.getRunnerId());

        pause(10000L);
        pause(10000L);
        pause(10000L);

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

    private void makeRunnerObsolete(String runnerId) {
        ydbConnector.sqlWrite("DECLARE $runner_id AS Text; "
                + "UPDATE `test1/mv_runners` "
                + "SET updated_at=Timestamp('2021-01-01T00:00:00Z') "
                + "WHERE runner_id=$runner_id;",
                Params.of("$runner_id", PrimitiveValue.newText(runnerId)));
    }

    private int workerThread(YdbConnector conn) {
        System.out.println("Worker entry");
        try {
            try (MvApi api = MvApi.newInstance(conn)) {
                var batchSettings = new MvBatchSettings(conn.getConfig().getProperties());
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
                        theCoord.stop();
                    } finally {
                        synchronized (workers) {
                            workers.remove(wi);
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
        props.setProperty(MvBatchSettings.CONF_REPORT_PERIOD_MS, "20000");
        props.setProperty(MvBatchSettings.CONF_RUNNER_TIMEOUT_MS, "40000");
//        props.setProperty("dump.threads.on.close", "true");
        return props;
    }

    private static void createDataTables(int part) {
        var sql = """
    CREATE TABLE `data_%1$d/main`(
        id Int32, sub_ref Int32, data1 Text,
        PRIMARY KEY(id),
        INDEX ix_sub GLOBAL ON (sub_ref));
    CREATE TABLE `data_%1$d/sub`(
        id Int32, main_ref Int32, data2 Text,
        PRIMARY KEY(id),
        INDEX ix_main GLOBAL ON (main_ref));
    CREATE TABLE `data_%1$d/mv`(
        id_main Int32, id_sub Int32, data1 Text, data2 Text,
        PRIMARY KEY(id_main))
                  """.formatted(part);
        runDdl(ydbConnector, sql);

        sql = """
    ALTER TABLE `data_%1$d/main` ADD CHANGEFEED `mv` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
    ALTER TABLE `data_%1$d/sub` ADD CHANGEFEED `mv` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
              """.formatted(part);
        runDdl(ydbConnector, sql);

        sql = """
    ALTER TOPIC `data_%1$d/main/mv` ADD CONSUMER c%1$d;
    ALTER TOPIC `data_%1$d/sub/mv` ADD CONSUMER c%1$d;
              """.formatted(part);
        runDdl(ydbConnector, sql);
    }

    private static void dropDataTables(int part) {
        var sql = """
    DROP TABLE `data_%1$d/main`;
    DROP TABLE `data_%1$d/sub`;
    DROP TABLE `data_%1$d/mv`;
                """.formatted(part);
        runDdl(ydbConnector, sql);
    }

    private static void configureMv(int part) {
        var sql = """
    $colindex = (SELECT COALESCE(MAX(statement_no),0) + 1 AS statement_no FROM `test1/statements`);
    UPSERT INTO `test1/statements` SELECT statement_no, @@
    CREATE ASYNC MATERIALIZED VIEW `data_%1$d/mv` AS
        SELECT main.id AS main_id, sub.id AS sub_id, main.data1 AS data1, sub.data2 AS data2
        FROM `data_%1$d/main` AS main
        LEFT JOIN `data_%1$d/sub` AS sub
          ON main.id = sub.main_ref;
    CREATE ASYNC HANDLER handler_%1$d CONSUMER c%1$d PROCESS `data_%1$d/mv`,
        INPUT `data_%1$d/main` CHANGEFEED mv AS STREAM,
        INPUT `data_%1$d/sub` CHANGEFEED mv AS STREAM;
        @@u AS statement_text
    FROM $colindex;
    UPSERT INTO `test1/mv_jobs`(job_name, should_run) VALUES('handler_%1$d'u, true);
                """.formatted(part);
        ydbConnector.sqlWrite(sql, Params.empty());
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
