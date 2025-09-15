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

    public MvApplyActionList(MvApplyAction action) {
        this.items = Collections.singletonList(action);
    }

    public MvApplyActionList(List<MvApplyAction> actions) {
        this.items = Collections.unmodifiableList(new ArrayList<>(actions));
    }

    public List<MvApplyAction> getItems() {
        return items;
    }

}
