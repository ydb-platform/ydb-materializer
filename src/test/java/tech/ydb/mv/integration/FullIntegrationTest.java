package tech.ydb.mv.integration;

import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.svc.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.mgt.MvBatchSettings;
import tech.ydb.mv.mgt.MvCoordinator;
import tech.ydb.mv.mgt.MvCoordinatorJobImpl;
import tech.ydb.mv.mgt.MvCoordinatorSettings;
import tech.ydb.mv.mgt.MvJobDao;
import tech.ydb.mv.mgt.MvLocker;
import tech.ydb.mv.mgt.MvRunner;

/**
 * @author Kirill Kurdyukov
 */
@Disabled
public class FullIntegrationTest extends AbstractIntegrationBase {

    @BeforeAll
    public static void init() {
        prepareDb();
    }

    @AfterAll
    public static void cleanup() {
        clearDb();
    }

    @Test
    /*
    [AAA] Database setup...
2025-09-17 21:58:28 INFO      1          YdbConnector:46   Connecting to grpc://localhost:58462/local...
[AAA] Preparation: creating tables...
[AAA] Preparation: adding consumers...
[AAA] Preparation: adding config...
2025-09-17 21:58:31 INFO      1          YdbConnector:207  Closing YDB connections...
2025-09-17 21:58:31 INFO      1          YdbConnector:236  Disconnected from YDB.
[AAA] Starting up...
2025-09-17 21:58:31 INFO      1          YdbConnector:46   Connecting to grpc://localhost:58462/local...
2025-09-17 21:58:31 INFO      1          YdbConnector:207  Closing YDB connections...
2025-09-17 21:58:31 INFO      1          YdbConnector:236  Disconnected from YDB.
        ...Sleeping for 100...
2025-09-17 21:58:31 INFO     67          YdbConnector:46   Connecting to grpc://localhost:58462/local...
2025-09-17 21:58:31 INFO     66          YdbConnector:46   Connecting to grpc://localhost:58462/local...
2025-09-17 21:58:31 INFO     67        MvConfigReader:50   Reading MV script from table test1/statements
2025-09-17 21:58:31 INFO     66        MvConfigReader:50   Reading MV script from table test1/statements
        ...Sleeping for 10000...
2025-09-17 21:58:31 INFO     68          YdbConnector:46   Connecting to grpc://localhost:58462/local...
2025-09-17 21:58:31 INFO     68        MvConfigReader:50   Reading MV script from table test1/statements
2025-09-17 21:58:32 INFO     68             MvService:286  Loading metadata and performing validation...
2025-09-17 21:58:33 INFO     67             MvService:286  Loading metadata and performing validation...
2025-09-17 21:58:33 INFO     66             MvService:286  Loading metadata and performing validation...
2025-09-17 21:58:35 INFO     68        MvConfigReader:50   Reading MV script from table test1/statements
2025-09-17 21:58:35 INFO     67        MvConfigReader:50   Reading MV script from table test1/statements
2025-09-17 21:58:35 INFO     66        MvConfigReader:50   Reading MV script from table test1/statements
2025-09-17 21:58:35 INFO     68             MvService:286  Loading metadata and performing validation...
2025-09-17 21:58:35 INFO     67             MvService:286  Loading metadata and performing validation...
2025-09-17 21:58:35 INFO     66             MvService:286  Loading metadata and performing validation...
2025-09-17 21:58:35 INFO     68              MvRunner:57   Starting MvRunner with ID: runner-28fdfca8-1758135515373
2025-09-17 21:58:35 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:35 INFO     68              MvRunner:65   MvRunner started successfully
        ...Sleeping for 40000...
2025-09-17 21:58:35 INFO     92              MvRunner:115  MvRunner thread started for runner: runner-28fdfca8-1758135515373
2025-09-17 21:58:35 INFO     66              MvRunner:57   Starting MvRunner with ID: runner-99faa7ea-1758135515372
2025-09-17 21:58:35 INFO     66              MvRunner:65   MvRunner started successfully
        ...Sleeping for 40000...
2025-09-17 21:58:35 INFO     94              MvRunner:115  MvRunner thread started for runner: runner-99faa7ea-1758135515372
2025-09-17 21:58:35 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:35 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:35 INFO     67              MvRunner:57   Starting MvRunner with ID: runner-05d28826-1758135515372
2025-09-17 21:58:35 INFO     67              MvRunner:65   MvRunner started successfully
        ...Sleeping for 40000...
2025-09-17 21:58:35 INFO     96              MvRunner:115  MvRunner thread started for runner: runner-05d28826-1758135515372
2025-09-17 21:58:35 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:35 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:35 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:36 INFO     93         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:36 INFO     91              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:36 INFO     91         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:36 INFO     91         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:36 INFO     91         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:36 INFO     95              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:36 INFO     95         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_2 — candidate for leader
2025-09-17 21:58:36 INFO     95         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_2
2025-09-17 21:58:36 INFO     95         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_2
2025-09-17 21:58:37 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:37 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:37 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:37 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:37 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:37 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:37 INFO     93         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:37 INFO     91              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:37 INFO     91         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:37 INFO     91         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:37 INFO     91         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:37 INFO     95              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:37 INFO     95         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_2 — candidate for leader
2025-09-17 21:58:37 INFO     95         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_2
2025-09-17 21:58:37 INFO     95         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_2
2025-09-17 21:58:38 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:38 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:38 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:38 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:38 INFO     93         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:38 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:38 INFO     91              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:38 INFO     91         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:38 INFO     91         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:38 INFO     91         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:38 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:38 INFO     95              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:38 INFO     95         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_2 — candidate for leader
2025-09-17 21:58:38 INFO     95         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_2
2025-09-17 21:58:38 INFO     95         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_2
2025-09-17 21:58:39 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:39 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:39 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:39 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:39 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:39 INFO     93         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:39 INFO     91              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:39 INFO     91         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:39 INFO     91         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:39 INFO     91         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:39 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:39 INFO     95              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:39 INFO     95         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_2 — candidate for leader
2025-09-17 21:58:39 INFO     95         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_2
2025-09-17 21:58:39 INFO     95         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_2
2025-09-17 21:58:40 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:40 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:40 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:40 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:40 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:40 INFO     93         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:40 INFO     91              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:40 INFO     91         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:40 INFO     91         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:40 INFO     91         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:40 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:40 INFO     95              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:40 INFO     95         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_2 — candidate for leader
2025-09-17 21:58:41 INFO     95         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_2
2025-09-17 21:58:41 INFO     95         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_2
2025-09-17 21:58:41 INFO      1          YdbConnector:46   Connecting to grpc://localhost:58462/local...
2025-09-17 21:58:41 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:41 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:41 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:41 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:41 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:41 INFO     93         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:42 INFO     91              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:42 INFO     91         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:42 INFO     91         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:42 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:42 INFO     91         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_1
2025-09-17 21:58:42 INFO     95              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:42 INFO     95         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_2 — candidate for leader
2025-09-17 21:58:42 INFO     95         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_2
2025-09-17 21:58:42 INFO     95         MvCoordinator:127  Lost semaphore ownership or runner_id mismatch, demoting, instanceId=instance_2
2025-09-17 21:58:42 INFO      1          YdbConnector:207  Closing YDB connections...
2025-09-17 21:58:42 INFO      1          YdbConnector:236  Disconnected from YDB.
        ...Sleeping for 20000...
2025-09-17 21:58:43 INFO     93              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:43 INFO     93              MvLocker:110  Lock `mv-coordinator-semaphore` obtained.
2025-09-17 21:58:43 INFO     93         MvCoordinator:79   Semaphore 'mv-coordinator-semaphore' acquired, instanceId=instance_1 — candidate for leader
2025-09-17 21:58:43 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:43 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:43 INFO     93         MvCoordinator:106  Becoming leader, starting leader loop, tick=1s, instanceId=instance_1
2025-09-17 21:58:44 INFO     93  MvCoordinatorJobImpl:166  Created START command for job: handler2 on runner: runner-05d28826-1758135515372
2025-09-17 21:58:44 INFO     93  MvCoordinatorJobImpl:166  Created START command for job: handler1 on runner: runner-28fdfca8-1758135515373
2025-09-17 21:58:44 INFO     93  MvCoordinatorJobImpl:109  Balanced jobs - stopped 0 extra, started 2 missing
2025-09-17 21:58:45 INFO     93  MvCoordinatorJobImpl:166  Created START command for job: handler2 on runner: runner-05d28826-1758135515372
2025-09-17 21:58:45 INFO     93  MvCoordinatorJobImpl:166  Created START command for job: handler1 on runner: runner-28fdfca8-1758135515373
2025-09-17 21:58:45 INFO     93  MvCoordinatorJobImpl:109  Balanced jobs - stopped 0 extra, started 2 missing
2025-09-17 21:58:45 INFO     96              MvRunner:182  Executing command: START for job: handler2
2025-09-17 21:58:45 INFO     92              MvRunner:182  Executing command: START for job: handler1
2025-09-17 21:58:46 INFO     96         MvApplyConfig:118  Configuring handler `handler2`, target `test1/mv2` ...
2025-09-17 21:58:46 INFO     92         MvApplyConfig:118  Configuring handler `handler1`, target `test1/mv1` ...
2025-09-17 21:58:46 INFO     96            ActionSync:59    [1] Handler `handler2`, target `test1/mv2`, input `test1/main_table` as `main`, changefeed `cf1` mode KEYS_ONLY
2025-09-17 21:58:46 INFO     92            ActionSync:59    [2] Handler `handler1`, target `test1/mv1`, input `test1/main_table` as `main`, changefeed `cf1` mode KEYS_ONLY
2025-09-17 21:58:46 INFO     92        ActionKeysGrab:32    [3] Handler `handler1`, target `test1/mv1`, input `test1/sub_table1` as `sub1`, changefeed `cf2` mode KEYS_ONLY
2025-09-17 21:58:46 INFO     96        ActionKeysGrab:32    [4] Handler `handler2`, target `test1/mv2`, input `test1/sub_table1` as `sub1`, changefeed `cf2` mode KEYS_ONLY
2025-09-17 21:58:46 INFO     96   ActionKeysTransform:36    [6] Handler `handler2`, target `test1/mv2`, input `test1/sub_table2` as `sub2`, changefeed `cf3` mode BOTH_IMAGES
2025-09-17 21:58:46 INFO     92   ActionKeysTransform:36    [5] Handler `handler1`, target `test1/mv1`, input `test1/sub_table2` as `sub2`, changefeed `cf3` mode BOTH_IMAGES
2025-09-17 21:58:46 INFO     96           MvCdcFeeder:41   Started 4 CDC reader threads for handler `handler2`
2025-09-17 21:58:46 INFO     92           MvCdcFeeder:41   Started 4 CDC reader threads for handler `handler1`
2025-09-17 21:58:46 INFO     92              MvLocker:89   Ensuring the single `handler1` job instance through lock with timeout PT5S...
2025-09-17 21:58:46 INFO     96              MvLocker:89   Ensuring the single `handler2` job instance through lock with timeout PT5S...
2025-09-17 21:58:46 INFO     92              MvLocker:110  Lock `handler1` obtained.
2025-09-17 21:58:46 INFO     96              MvLocker:110  Lock `handler2` obtained.
2025-09-17 21:58:46 INFO     92       MvJobController:67   Starting the controller `handler1`
2025-09-17 21:58:46 INFO     96       MvJobController:67   Starting the controller `handler2`
2025-09-17 21:58:46 INFO     92        MvApplyManager:121  Started 4 apply worker(s) for handler `handler1`.
2025-09-17 21:58:46 INFO     92           MvCdcFeeder:57   Activating the CDC reader for feeder `handler1`
2025-09-17 21:58:46 INFO     96        MvApplyManager:121  Started 4 apply worker(s) for handler `handler2`.
2025-09-17 21:58:46 INFO     96           MvCdcFeeder:57   Activating the CDC reader for feeder `handler2`
2025-09-17 21:58:46 INFO    116      MvCdcEventReader:32   Feeder `handler1` topic `/local/test1/sub_table1/cf2` session 2 for partition 0 onStart with last committed offset 0
2025-09-17 21:58:46 INFO    115      MvCdcEventReader:32   Feeder `handler1` topic `/local/test1/sub_table2/cf3` session 3 for partition 0 onStart with last committed offset 0
2025-09-17 21:58:46 INFO    117      MvCdcEventReader:32   Feeder `handler2` topic `/local/test1/sub_table2/cf3` session 2 for partition 0 onStart with last committed offset 0
2025-09-17 21:58:46 INFO    118      MvCdcEventReader:32   Feeder `handler1` topic `/local/test1/main_table/cf1` session 1 for partition 0 onStart with last committed offset 0
2025-09-17 21:58:46 INFO    119      MvCdcEventReader:32   Feeder `handler2` topic `/local/test1/main_table/cf1` session 1 for partition 0 onStart with last committed offset 0
2025-09-17 21:58:46 INFO    120      MvCdcEventReader:32   Feeder `handler2` topic `/local/test1/sub_table1/cf2` session 3 for partition 0 onStart with last committed offset 0
2025-09-17 21:58:47 INFO     93  MvCoordinatorJobImpl:166  Created START command for job: handler2 on runner: runner-05d28826-1758135515372
2025-09-17 21:58:47 INFO     92              MvRunner:241  Started handler: handler1
2025-09-17 21:58:47 INFO     92              MvRunner:196  Command executed successfully: START for job: handler1
2025-09-17 21:58:47 INFO     96              MvRunner:241  Started handler: handler2
2025-09-17 21:58:47 INFO     96              MvRunner:196  Command executed successfully: START for job: handler2
2025-09-17 21:58:47 INFO     92              MvRunner:182  Executing command: START for job: handler1
2025-09-17 21:58:47 INFO     96              MvRunner:182  Executing command: START for job: handler2
2025-09-17 21:58:47 INFO     93  MvCoordinatorJobImpl:166  Created START command for job: handler1 on runner: runner-28fdfca8-1758135515373
2025-09-17 21:58:47 INFO     93  MvCoordinatorJobImpl:109  Balanced jobs - stopped 0 extra, started 2 missing
2025-09-17 21:58:47 WARN     92       MvJobController:60   Ignored start call for an already running controller `handler1`
2025-09-17 21:58:47 WARN     96       MvJobController:60   Ignored start call for an already running controller `handler2`
2025-09-17 21:58:47 INFO     96              MvRunner:196  Command executed successfully: START for job: handler2
2025-09-17 21:58:47 INFO     92              MvRunner:196  Command executed successfully: START for job: handler1
2025-09-17 21:58:48 INFO     91              MvLocker:104  Failed to obtain lock `mv-coordinator-semaphore`, concurrent job instance seems to be running.
2025-09-17 21:58:48 INFO     95              MvLocker:104  Failed to obtain lock `mv-coordinator-semaphore`, concurrent job instance seems to be running.
2025-09-17 21:58:49 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:49 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:51 INFO     92              MvRunner:182  Executing command: START for job: handler1
2025-09-17 21:58:51 INFO     96              MvRunner:182  Executing command: START for job: handler2
2025-09-17 21:58:51 WARN     92       MvJobController:60   Ignored start call for an already running controller `handler1`
2025-09-17 21:58:51 WARN     96       MvJobController:60   Ignored start call for an already running controller `handler2`
2025-09-17 21:58:51 INFO     92              MvRunner:196  Command executed successfully: START for job: handler1
2025-09-17 21:58:51 INFO     96              MvRunner:196  Command executed successfully: START for job: handler2
2025-09-17 21:58:54 INFO     91              MvLocker:104  Failed to obtain lock `mv-coordinator-semaphore`, concurrent job instance seems to be running.
2025-09-17 21:58:54 INFO     95              MvLocker:104  Failed to obtain lock `mv-coordinator-semaphore`, concurrent job instance seems to be running.
2025-09-17 21:58:55 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:58:55 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:59:00 INFO     91              MvLocker:104  Failed to obtain lock `mv-coordinator-semaphore`, concurrent job instance seems to be running.
2025-09-17 21:59:00 INFO     95              MvLocker:104  Failed to obtain lock `mv-coordinator-semaphore`, concurrent job instance seems to be running.
2025-09-17 21:59:01 INFO     91              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
2025-09-17 21:59:01 INFO     95              MvLocker:89   Ensuring the single `mv-coordinator-semaphore` job instance through lock with timeout PT5S...
[AAA] Database cleanup...
2025-09-17 21:59:02 INFO      1          YdbConnector:46   Connecting to grpc://localhost:58462/local...
2025-09-17 21:59:04 INFO    113      MvCdcEventReader:50   Feeder `handler1` topic `/local/test1/main_table/cf1` session 1 onClosed
2025-09-17 21:59:04 INFO    114      MvCdcEventReader:50   Feeder `handler2` topic `/local/test1/main_table/cf1` session 1 onClosed
2025-09-17 21:59:04 INFO    117      MvCdcEventReader:50   Feeder `handler2` topic `/local/test1/sub_table1/cf2` session 3 onClosed
2025-09-17 21:59:04 INFO    116      MvCdcEventReader:50   Feeder `handler1` topic `/local/test1/sub_table1/cf2` session 2 onClosed
2025-09-17 21:59:04 INFO    118      MvCdcEventReader:50   Feeder `handler1` topic `/local/test1/sub_table2/cf3` session 3 onClosed
2025-09-17 21:59:04 INFO    119      MvCdcEventReader:50   Feeder `handler2` topic `/local/test1/sub_table2/cf3` session 2 onClosed
2025-09-17 21:59:04 INFO      1          YdbConnector:207  Closing YDB connections...
2025-09-17 21:59:04 INFO      1          YdbConnector:236  Disconnected from YDB.
     */
    public void concurrencyIntegrationTest() {
        System.err.println("[AAA] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        var batchSettings = new MvBatchSettings(cfg.getProperties());
        cfg.getProperties().setProperty(MvConfig.CONF_COORD_TIMEOUT, "5");
        var instance1 = "instance_1";
        var instance2 = "instance_2";

        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, """
            CREATE TABLE `mv_jobs` (
                job_name Text NOT NULL,
                job_settings JsonDocument,
                should_run Bool,
                runner_id Text,
                PRIMARY KEY(job_name)
            );

            CREATE TABLE `mv_runners` (
                runner_id Text NOT NULL,
                runner_identity Text,
                updated_at Timestamp,
                PRIMARY KEY(runner_id)
            );

            CREATE TABLE `mv_runner_jobs` (
                runner_id Text NOT NULL,
                job_name Text NOT NULL,
                job_settings JsonDocument,
                started_at Timestamp,
                PRIMARY KEY(runner_id, job_name)
            );

            CREATE TABLE `mv_commands` (
                runner_id Text NOT NULL,
                command_no Uint64 NOT NULL,
                created_at Timestamp,
                command_type Text,
                job_name Text,
                job_settings JsonDocument,
                command_status Text,
                command_diag Text,
                PRIMARY KEY(runner_id, command_no)
            );
                    """);
        }

        Thread t1 = new Thread(() -> handler(cfg, instance1, batchSettings));
        Thread t2 = new Thread(() -> handler(cfg, instance2, batchSettings));
        Thread t1dup = new Thread(() -> handler(cfg, instance1, batchSettings));

        t1.start();
        t2.start();
        pause(100L);
        t1dup.start();

        pause(10_000);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, """
                    INSERT INTO `mv_jobs` (job_name, should_run) VALUES
                        ('sys$coordinator', true),
                        ('handler1', true),
                        ('handler2', true);
                        """);
        }
        pause(20_000);
    }

    private void handler(YdbConnector.Config cfg, String instanceName, MvBatchSettings batchSettings) {
        try (YdbConnector conn = new YdbConnector(cfg); var mvService = new MvService(conn); var runner = new MvRunner(conn, new MvService(conn))) {
            var mvLocker = new MvLocker(conn);
            new MvCoordinator(
                    mvLocker,
                    Executors.newScheduledThreadPool(1),
                    conn.getQueryRetryCtx(),
                    new MvCoordinatorSettings(1),
                    instanceName,
                    new MvCoordinatorJobImpl(new MvJobDao(conn, batchSettings), batchSettings)
            ).start();
            runner.start();

            pause(40_000);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }
}
