package tech.ydb.mv.mgt;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;

/**
 * @author Kirill Kurdyukov
 */
public class MvCoordinatorTest extends MgmtTestBase {

    @BeforeAll
    public static void setup() {
        prepareMgtDb();
    }

    @AfterAll
    public static void cleanup() {
        clearMgtDb();
    }

    @BeforeEach
    public void prepareEach() {
        refreshBeforeRun();
    }

    private MvBatchSettings getSettings() {
        MvBatchSettings v = new MvBatchSettings();
        v.setCoordStartupMs(0L);
        v.setScanPeriodMs(200L);
        v.setTableCommands("test1/mv_commands");
        v.setTableJobs("test1/mv_jobs");
        v.setTableRunners("test1/mv_runners");
        v.setTableRunnerJobs("test1/mv_runner_jobs");
        return v;
    }

    @Test
    public void checkSingleThreaded() {
        System.out.println("========= Start single-threaded coordination test");

        var cfg = MvConfig.fromBytes(getConfigBytes(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg, true)) {
            runSingle(conn);
        }
    }

    private void runSingle(YdbConnector conn) {
        final var queue = new ConcurrentLinkedQueue<Integer>();
        final var deque = new ConcurrentLinkedDeque<String>();

        MvCoordinator coord = null;
        try {
            coord = MvCoordinator.newInstance(conn, getSettings(), "instance", null,
                    new MvCoordinatorActions() {
                private final AtomicReference<String> tick = new AtomicReference<>(UUID.randomUUID().toString());

                @Override
                public void onStart() {
                    queue.add(1);
                }

                @Override
                public void onTick() {
                    var peekLast = deque.peekLast();

                    if (peekLast == null) {
                        deque.addLast(tick.get());
                        return;
                    }

                    if (!tick.get().equals(peekLast)) {
                        deque.addLast(tick.get());
                    }
                }

                @Override
                public void onStop() {
                    tick.set(UUID.randomUUID().toString());
                }
            });

            coord.start();

            for (int i = 1; i <= 10; i++) {
                pause(1_001);
                coord.stop();
                pause(1_002);
                coord.start();
                pause(1_003);
                Assertions.assertEquals(i + 1, queue.size());
                Assertions.assertEquals(i + 1, deque.size());
            }
        } finally {
            if (coord != null) {
                coord.close();
            }
        }
    }

    @Test
    public void checkMultiThreaded() {
        System.out.println("========= Start multi-threaded coordination test");

        var cfg = MvConfig.fromBytes(getConfigBytes(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg, true)) {
            runMulti(conn);
        }
    }

    private void runMulti(YdbConnector conn) {
        final var queue = new ConcurrentLinkedQueue<Integer>();
        final var deque = new ConcurrentLinkedDeque<String>();
        final ArrayList<MvCoordinator> coords = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            MvCoordinator c = MvCoordinator.newInstance(conn, getSettings(), "instance_" + i, null,
                    new MvCoordinatorActions() {
                private final AtomicReference<String> tick = new AtomicReference<>(UUID.randomUUID().toString());

                @Override
                public void onStart() {
                    queue.add(1);
                }

                @Override
                public void onTick() {
                    var peekLast = deque.peekLast();

                    if (peekLast == null) {
                        deque.addLast(tick.get());
                        return;
                    }

                    if (!tick.get().equals(peekLast)) {
                        deque.addLast(tick.get());
                    }
                }

                @Override
                public void onStop() {
                    tick.set(UUID.randomUUID().toString());
                }
            });

            c.start();
            coords.add(c);
        }

        try {
            for (int i = 1; i <= 10; i++) {
                pause(1_001);
                stopActive(coords);
                pause(1_002);
                Assertions.assertEquals(i + 1, queue.size());
                Assertions.assertEquals(i + 1, deque.size());
            }
        } finally {
            for (var c : coords) {
                c.close();
            }
        }
    }

    private void stopActive(ArrayList<MvCoordinator> coords) {
        MvCoordinator active = null;
        for (var c : coords) {
            if (c.isLeader()) {
                if (active == null) {
                    active = c;
                } else {
                    Assertions.assertFalse(true, "Multiple active coordinators: "
                            + active.getRunnerId() + " vs " + c.getRunnerId());
                }
            }
        }
        if (active != null) {
            System.out.println("Stopping instance " + active.getRunnerId());
            active.stop();
        }
        Assertions.assertNotNull(active, "Missing active coordinator");
    }

}
