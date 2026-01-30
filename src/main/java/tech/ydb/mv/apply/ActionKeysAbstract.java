package tech.ydb.mv.apply;

import java.util.List;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTarget;

/**
 * Generic code to obtain the keys to the main table via another join input.
 *
 * @author zinal
 */
abstract class ActionKeysAbstract extends ActionBase implements MvApplyAction {

    protected final MvTarget target;
    protected final String inputTableName;
    protected final String inputTableAlias;
    protected final MvKeyInfo keyInfo;

    public ActionKeysAbstract(MvTarget target, MvJoinSource src,
            MvTarget transformation, MvActionContext context) {
        super(context);
        if (target == null || src == null || src.getChangefeedInfo() == null
                || transformation == null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.target = target;
        this.inputTableName = src.getTableName();
        this.inputTableAlias = src.getTableAlias();
        this.keyInfo = target.getTopMostSource().getTableInfo().getKeyInfo();
        setMetricsScope(target.getName(), inputTableName, inputTableAlias);
        if (this.keyInfo.size() != transformation.getColumns().size()) {
            throw new IllegalArgumentException("Illegal key setup, expected "
                    + this.keyInfo.size() + ", got " + this.keyInfo.size());
        }
    }

    @Override
    public final void apply(List<MvApplyTask> input) {
        new PerCommit(input).apply((handler, tasks) -> process(handler, tasks));
    }

    /**
     * Process the apply tasks grouped by the commit handler.
     *
     * @param handler The commit handler
     * @param tasks The apply tasks all related to the commit handler passed
     */
    protected abstract void process(MvCommitHandler handler, List<MvApplyTask> tasks);

}
