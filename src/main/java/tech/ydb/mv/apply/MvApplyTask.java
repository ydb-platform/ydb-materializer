package tech.ydb.mv.apply;

import java.util.List;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.data.MvChangeRecord;

/**
 *
 * @author zinal
 */
public class MvApplyTask {

    private final MvChangeRecord data;
    private final MvApplyActionList actions;
    private final MvCommitHandler commit;
    private final int workerId;

    public MvApplyTask(MvChangeRecord data, MvCommitHandler commit,
            MvApplyActionList actions, int workerId) {
        this.data = data;
        this.actions = actions;
        this.commit = commit;
        this.workerId = workerId;
    }

    public MvApplyTask(MvChangeRecord data, MvCommitHandler commit,
            List<MvApplyAction> actions, int workerId) {
        this.data = data;
        this.actions = new MvApplyActionList(actions);
        this.commit = commit;
        this.workerId = workerId;
    }

    public MvChangeRecord getData() {
        return data;
    }

    public List<MvApplyAction> getActions() {
        return actions.getItems();
    }

    public MvCommitHandler getCommit() {
        return commit;
    }

    public int getWorkerId() {
        return workerId;
    }

    @Override
    public String toString() {
        return "MvApplyTask{" + data + '}';
    }

}
