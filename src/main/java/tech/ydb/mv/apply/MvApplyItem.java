package tech.ydb.mv.apply;

import tech.ydb.mv.model.MvKeyValue;

/**
 *
 * @author zinal
 */
public class MvApplyItem {

    private final MvKeyValue data;
    private final MvCommitHandler commit;
    private final MvApplyConfig apply;

    public MvApplyItem(MvKeyValue data, MvCommitHandler commit, MvApplyConfig apply) {
        this.data = data;
        this.commit = commit;
        this.apply = apply;
    }

    public MvKeyValue getData() {
        return data;
    }

    public MvCommitHandler getCommit() {
        return commit;
    }

    public MvApplyConfig getApply() {
        return apply;
    }

}
