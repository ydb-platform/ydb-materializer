package tech.ydb.mv.apply;

import java.util.List;

import com.google.common.collect.Lists;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvViewExpr;

/**
 * Generic code to obtain the keys to the target table via some join input.
 *
 * @author zinal
 */
abstract class ActionKeysAbstract extends ActionBase implements MvApplyAction {

    protected final MvViewExpr target;
    protected final String inputTableName;
    protected final String inputTableAlias;
    protected final MvKeyInfo keyInfo;
    protected final int selectBatchSize;

    public ActionKeysAbstract(MvViewExpr target, MvJoinSource src,
            MvViewExpr transformation, MvActionContext context, MetricsScope metricsScope) {
        super(context, metricsScope);
        if (target == null || src == null || src.getChangefeedInfo() == null
                || transformation == null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.target = target;
        this.inputTableName = src.getTableName();
        this.inputTableAlias = src.getTableAlias();
        this.keyInfo = target.getTopMostSource().getTableInfo().getKeyInfo();
        if (this.keyInfo.size() != transformation.getColumns().size()) {
            throw new IllegalArgumentException("Illegal key setup, expected "
                    + this.keyInfo.size() + ", got " + this.keyInfo.size());
        }
        this.selectBatchSize = context.getSettings().getSelectBatchSize();
    }

    @Override
    public final void apply(List<MvApplyTask> input) {
        new PerCommit(input).apply((handler, tasks) -> applyPerCommit(handler, tasks));
    }

    private void applyPerCommit(MvCommitHandler handler, List<MvApplyTask> tasks) {
        for (List<MvApplyTask> part : Lists.partition(tasks, selectBatchSize)) {
            process(handler, part);
        }
    }

    /**
     * Process the apply tasks grouped by the commit handler.
     *
     * @param handler The commit handler
     * @param tasks The apply tasks all related to the commit handler passed
     */
    protected abstract void process(MvCommitHandler handler, List<MvApplyTask> tasks);

}
