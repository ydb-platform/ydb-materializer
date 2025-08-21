package tech.ydb.mv.apply;

import tech.ydb.mv.model.MvKey;

/**
 *
 * @author zinal
 */
public class MvChangeRecord {

    private final MvKey key;
    private final MvCommitHandler commit;
    private final MvApplyConfig apply;

    public MvChangeRecord(MvKey key, MvCommitHandler commit, MvApplyConfig apply) {
        this.key = key;
        this.commit = commit;
        this.apply = apply;
    }

    public MvKey getKey() {
        return key;
    }

    public MvCommitHandler getCommit() {
        return commit;
    }

    public MvApplyConfig getApply() {
        return apply;
    }

}
