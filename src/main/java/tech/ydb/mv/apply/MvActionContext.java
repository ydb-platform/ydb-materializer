package tech.ydb.mv.apply;

import tech.ydb.query.tools.SessionRetryContext;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.svc.MvJobContext;

/**
 * The context to execute the apply actions.
 *
 * @author zinal
 */
class MvActionContext {

    private final MvJobContext base;
    private final MvApplyManager applyManager;
    private final SessionRetryContext retryCtx;

    public MvActionContext(MvJobContext base, MvApplyManager applyManager) {
        this.base = base;
        this.applyManager = applyManager;
        this.retryCtx = base.getYdb().getQueryRetryCtx();
    }

    public MvConfig.PartitioningStrategy getPartitioning() {
        String v = base.getService().getYdb().getProperty(MvConfig.CONF_PARTITIONING);
        MvConfig.PartitioningStrategy partitioning = MvConfig.parsePartitioning(v);
        if (partitioning == null) {
            return MvConfig.PartitioningStrategy.HASH;
        }
        return MvConfig.PartitioningStrategy.RANGE;
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public MvHandlerSettings getSettings() {
        return base.getSettings();
    }

    public MvHandler getHandler() {
        return base.getHandler();
    }

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public boolean isRunning() {
        return base.isRunning();
    }

}
