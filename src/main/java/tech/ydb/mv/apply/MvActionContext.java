package tech.ydb.mv.apply;

import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;

/**
 * The context to execute the apply actions.
 *
 * @author zinal
 */
public class MvActionContext {

    private final MvJobContext base;
    private final MvApplyManager applyManager;
    private final QueryClient queryClient;
    private final SessionRetryContext retryCtx;

    public MvActionContext(MvJobContext base, MvApplyManager applyManager) {
        this.base = base;
        this.applyManager = applyManager;
        this.queryClient = base.getConnector().getQueryClient();
        this.retryCtx = SessionRetryContext.create(this.queryClient)
                .idempotent(true).build();
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public MvHandlerSettings getSettings() {
        return base.getSettings();
    }

    public MvHandler getMetadata() {
        return base.getMetadata();
    }

    public QueryClient getQueryClient() {
        return queryClient;
    }

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public boolean isRunning() {
        return base.isRunning();
    }

}
