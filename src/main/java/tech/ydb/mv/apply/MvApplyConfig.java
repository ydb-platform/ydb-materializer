package tech.ydb.mv.apply;

import java.util.ArrayList;
import tech.ydb.mv.model.MvTableInfo;

/**
 * The apply configuration includes the settings to process the change records
 * coming from the single input table.
 *
 * @author zinal
 */
public class MvApplyConfig {
    private final MvTableInfo table;
    private final MvWorkerSelector selector;
    private final ArrayList<MvApplyAction> actions = new ArrayList<>();

    public MvApplyConfig(MvTableInfo table, int workerCount) {
        this.table = table;
        this.selector = new MvWorkerSelector(table, workerCount);
    }

    public MvTableInfo getTable() {
        return table;
    }

    public MvWorkerSelector getSelector() {
        return selector;
    }

    public ArrayList<MvApplyAction> getActions() {
        return actions;
    }

}
