package tech.ydb.mv;

import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.apply.MvApplyWorkerPool;
import tech.ydb.mv.model.MvHandler;

/**
 * The controller logic for a single handler.
 * Combines the topic reader, apply manager, ...
 *
 * @author zinal
 */
public class MvHandlerController {

    private final MvHandler metadata;
    private final MvApplyManager applyManager;

    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvHandlerController(MvHandler metadata, MvApplyWorkerPool workerPool) {
        this.metadata = metadata;
        this.applyManager = new MvApplyManager(this, workerPool);
    }

    public MvHandler getMetadata() {
        return metadata;
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public void start() {
        shouldRun.set(true);
    }

    public void stop() {
        shouldRun.set(false);
    }

}
