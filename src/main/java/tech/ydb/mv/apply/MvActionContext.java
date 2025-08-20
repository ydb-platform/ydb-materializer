package tech.ydb.mv.apply;

import tech.ydb.query.QueryClient;

import tech.ydb.mv.model.MvWorkSettings;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * The context to execute actions.
 *
 * @author zinal
 */
public class MvActionContext {

    private final MvWorkSettings settings;
    private final QueryClient queryClient;
    private final SessionRetryContext retryCtx;

    public MvActionContext(MvWorkSettings settings, QueryClient queryClient) {
        this.settings = settings;
        this.queryClient = queryClient;
        this.retryCtx = SessionRetryContext.create(queryClient)
                .idempotent(true).build();
    }

    public MvWorkSettings getSettings() {
        return settings;
    }

    public QueryClient getQueryClient() {
        return queryClient;
    }

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

}
