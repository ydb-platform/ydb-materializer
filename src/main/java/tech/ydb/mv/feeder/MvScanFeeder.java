package tech.ydb.mv.feeder;

import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.model.MvTarget;

/**
 * Scan feeder reads the keys from the topmost-left source of a MV.
 *
 * @author zinal
 */
public class MvScanFeeder {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvScanFeeder.class);

    private final MvJobContext context;
    private final MvApplyManager applyManager;
    private final MvTarget target;
    private final AtomicBoolean shouldRun;
    private final Thread thread;

    public MvScanFeeder(MvJobContext context, MvApplyManager applyManager, MvTarget target) {
        this.context = context;
        this.applyManager = applyManager;
        this.target = target;
        this.shouldRun = new AtomicBoolean(false);
        this.thread = new Thread(() -> run());
        this.thread.setDaemon(true);
        this.thread.setName("ydb-scan-feeder-"
                + context.getMetadata().getName()
                + "-" + target.getName());
        LOG.info("Started scan feeder for target {} in handler {}",
                target.getName(), context.getMetadata().getName());
    }

    public boolean isRunning() {
        return shouldRun.get() && context.isRunning();
    }

    public void start() {
        if (thread.isAlive()) {
            return;
        }
        if (! context.isRunning()) {
            return;
        }
        shouldRun.set(true);
        thread.start();
    }

    public void stop() {
        shouldRun.set(false);
    }

    private void run() {
        initScan();
        while (isRunning()) {
            if (stepScan()) {
                break;
            }
        }
        completeScan();
    }

    private boolean stepScan() {
        return false;
    }

    private void initScan() {

    }

    private void completeScan() {

    }

}
