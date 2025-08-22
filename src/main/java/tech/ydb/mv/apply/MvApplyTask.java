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

    private int errorCount = 0;

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

    public int getErrorCount() {
        return errorCount;
    }

    public void clearErrors() {
        errorCount = 0;
    }

    public void markError() {
        errorCount += 1;
    }
}
