package tech.ydb.mv.apply;

import java.util.List;

import tech.ydb.mv.model.MvChangeRecord;

/**
 * The apply action performs the desired apply action on the particular input.
 *
 * @author zinal
 */
public interface MvApplyAction {

    void apply(List<MvChangeRecord> input);

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

}
