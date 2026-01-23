package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Read-only list of actions to be applied.
 *
 * @author zinal
 */
public class MvApplyActionList {

    private final List<MvApplyAction> items;

    /**
     * Create the list of actions containing a single specific action.
     *
     * @param action Action
     */
    public MvApplyActionList(MvApplyAction action) {
        this.items = Collections.singletonList(action);
    }

    /**
     * Create the list of actions containing the specified actions.
     *
     * @param actions List of actions
     */
    public MvApplyActionList(List<MvApplyAction> actions) {
        this.items = Collections.unmodifiableList(new ArrayList<>(actions));
    }

    /**
     * @return Actions contained within the list
     */
    public List<MvApplyAction> getItems() {
        return items;
    }

}
