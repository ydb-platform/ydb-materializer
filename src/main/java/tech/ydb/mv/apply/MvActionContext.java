package tech.ydb.mv.apply;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.svc.MvJobContext;

/**
 * The shared context to execute the apply actions.
 *
 * Combines the job context and the apply manager.
 *
 * @author zinal
 */
class MvActionContext {

    private final MvJobContext jobContext;
    private final MvApplyManager applyManager;

    public MvActionContext(MvJobContext base, MvApplyManager applyManager) {
        this.jobContext = base;
        this.applyManager = applyManager;
    }

    public MvJobContext getJobContext() {
        return jobContext;
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public MvHandlerSettings getSettings() {
        return jobContext.getSettings();
    }

    public MvHandler getHandler() {
        return jobContext.getHandler();
    }

    public boolean isRunning() {
        return jobContext.isRunning();
    }

}
