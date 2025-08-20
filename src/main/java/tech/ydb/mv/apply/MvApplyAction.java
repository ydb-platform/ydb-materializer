package tech.ydb.mv.apply;

import java.util.Collection;

/**
 * The apply action performs the desired apply action on the particular input.
 *
 * @author zinal
 */
public interface MvApplyAction {

    void apply(Collection<MvApplyItem> item);

}
