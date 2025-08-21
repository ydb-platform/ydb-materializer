package tech.ydb.mv.apply;

import tech.ydb.mv.model.MvChangeRecord;

/**
 *
 * @author zinal
 */
public class MvApplyTask {

    private final MvChangeRecord data;
    private final MvApplyConfig actions;
    private final MvCommitHandler commit;

    public MvApplyTask(MvChangeRecord data, MvApplyConfig actions,
            MvCommitHandler commit) {
        this.data = data;
        this.actions = actions;
        this.commit = commit;
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

}
