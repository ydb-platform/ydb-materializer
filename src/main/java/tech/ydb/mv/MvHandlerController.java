package tech.ydb.mv;

import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.apply.MvApplyWorkerPool;
import tech.ydb.mv.feeder.MvCdcReader;
import tech.ydb.mv.feeder.MvCdcThreadPool;
import tech.ydb.mv.model.MvHandler;

/**
 * The controller logic for a single handler.
 * Combines the topic reader, apply manager, ...
 *
 * @author zinal
 */
public class MvHandlerController {

    private final YdbConnector connector;
    private final MvHandler metadata;
    private final MvApplyManager applyManager;
    private final MvCdcReader cdcReader;

    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvHandlerController(YdbConnector connector,
            MvApplyWorkerPool workerPool, MvCdcThreadPool cdcPool,
            MvHandler metadata) {
        this.connector = connector;
        this.metadata = metadata;
        this.applyManager = new MvApplyManager(this, workerPool);
        this.cdcReader = new MvCdcReader(this, cdcPool);
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvHandler getMetadata() {
        return metadata;
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public MvCdcReader getCdcReader() {
        return cdcReader;
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public void start() {
        shouldRun.set(true);
        cdcReader.start();
    }

    public void stop() {
        shouldRun.set(false);
        cdcReader.stop();
    }

}
