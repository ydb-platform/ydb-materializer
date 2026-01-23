package tech.ydb.mv.apply;

import java.util.List;

/**
 * The apply action performs the desired apply action on the particular input.
 *
 * @author zinal
 */
public interface MvApplyAction {

    /**
     * Apply the action to the list of input tasks.
     *
     * @param input List of tasks (data items) to be processed by the action.
     */
    void apply(List<MvApplyTask> input);

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

}
