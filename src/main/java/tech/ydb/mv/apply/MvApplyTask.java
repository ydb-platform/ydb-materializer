package tech.ydb.mv.apply;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvChangeRecord;

/**
 *
 * @author zinal
 */
public class MvApplyTask {

    private final MvChangeRecord data;
    private final MvApplyConfig actions;
    private final MvCommitHandler commit;
    private final int workerId;

    public MvApplyTask(MvChangeRecord data, MvApplyConfig actions,
            MvCommitHandler commit) {
        this.data = data;
        this.actions = actions;
        this.commit = commit;
        this.workerId = actions.getSelector().choose(data.getKey());
    }

    public MvChangeRecord getData() {
        return data;
    }

    public MvApplyConfig getActions() {
        return actions;
    }

    public MvCommitHandler getCommit() {
        return commit;
    }

    public int getWorkerId() {
        return workerId;
    }

}
