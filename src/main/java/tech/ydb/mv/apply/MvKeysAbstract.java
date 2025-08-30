package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTarget;

/**
 * Generic code to obtain the keys to the main table via another join input.
 *
 * @author zinal
 */
public abstract class MvKeysAbstract extends MvActionBase implements MvApplyAction {

    protected final MvTarget target;
    protected final MvTarget transformation;
    protected final String inputTableName;
    protected final String inputTableAlias;
    protected final MvKeyInfo keyInfo;

    public MvKeysAbstract(MvTarget target, MvJoinSource src,
            MvTarget transformation, MvActionContext context) {
        super(context);
        if (target==null || src==null || src.getChangefeedInfo()==null
                || transformation==null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.target = target;
        this.transformation = transformation;
        this.inputTableName = src.getTableName();
        this.inputTableAlias = src.getTableAlias();
        this.keyInfo = target.getSources().get(0).getTableInfo().getKeyInfo();
        if (this.keyInfo.size() != this.transformation.getColumns().size()) {
            throw new IllegalArgumentException("Illegal key setup, expected "
                    + this.keyInfo.size() + ", got " + this.keyInfo.size());
        }
    }

    @Override
    public final void apply(List<MvApplyTask> input) {
        new PerCommit(input).apply();
    }

    /**
     * Process the apply tasks grouped by the commit handler.
     *
     * @param handler The commit handler
     * @param tasks The apply tasks all related to the commit handler passed
     */
    protected abstract void process(MvCommitHandler handler, List<MvApplyTask> tasks);

    /**
     * Group the input records by commit handlers.
     * This enables more efficient per-commit-handler behavior.
     */
    private class PerCommit {
        final HashMap<MvCommitHandler, ArrayList<MvApplyTask>> items = new HashMap<>();

        PerCommit(List<MvApplyTask> tasks) {
            for (MvApplyTask task : tasks) {
                ArrayList<MvApplyTask> cur = items.get(task.getCommit());
                if (cur==null) {
                    cur = new ArrayList<>();
                    items.put(task.getCommit(), cur);
                }
                cur.add(task);
            }
        }

        void apply() {
            items.forEach((handler, tasks) -> process(handler, tasks));
        }
    }
}
