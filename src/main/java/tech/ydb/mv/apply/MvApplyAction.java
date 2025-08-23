package tech.ydb.mv.apply;

import java.util.List;

/**
 * The apply action performs the desired apply action on the particular input.
 *
 * @author zinal
 */
public interface MvApplyAction {

    void apply(List<MvApplyTask> input);

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

}
