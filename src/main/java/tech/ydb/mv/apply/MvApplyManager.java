package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import tech.ydb.mv.MvHandlerController;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvKeyValue;
import tech.ydb.mv.util.YdbMisc;

/**
 * The apply manager processes the changes in the context of a single handler.
 * Multiple apply managers can run in a single application, typically
 * using a single shared pool of workers.
 *
 * @author zinal
 */
public class MvApplyManager {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyManager.class);

    private final MvHandlerController controller;
    private final MvApplyWorkerPool workers;

    // table name -> table apply configuration data
    private final HashMap<String, MvApplyConfig> applyConfig = new HashMap<>();

    public MvApplyManager(MvHandlerController controller, MvApplyWorkerPool workers) {
        this.controller = controller;
        this.workers = workers;
        buildConfig();
    }

    private void buildConfig() {
        for (MvInput mi : controller.getMetadata().getInputs().values()) {
            MvApplyConfig mac = applyConfig.get(mi.getTableName());
            if (mac==null) {
                mac = new MvApplyConfig(mi.getTableInfo(), workers.getCount());
                applyConfig.put(mi.getTableName(), mac);
            }
        }
    }

    /**
     * Insert the input data to the queues of the proper workers.
     *
     * In case some of the queues are full, wait until the capacity
     * becomes available, or until the update manager is stopped.
     *
     * @param keys The keys to be put to the queues.
     * @param commitHandler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    public boolean submit(Collection<MvKeyValue> keys, MvCommitHandler commitHandler) {
        ArrayList<MvApplyItem> curr = new ArrayList<>(keys.size());
        ArrayList<MvApplyItem> next = new ArrayList<>(keys.size());
        for (MvKeyValue mkv : keys) {
            MvApplyConfig apply = applyConfig.get(mkv.getTableInfo().getName());
            if (apply==null) {
                LOG.warn("Skipping record for unknown table {}", mkv.getTableInfo().getName());
                continue;
            }
            curr.add(new MvApplyItem(mkv, commitHandler, apply));
        }
        while (controller.isRunning()) {
            for (MvApplyItem item : curr) {
                int workerId = item.getApply().getSelector().choose(item.getData());
                if (! workers.get(workerId).submit(item)) {
                    // add for re-processing
                    next.add(item);
                }
            }
            if (next.isEmpty()) {
                return true;
            }
            // Allow the queues to get released.
            YdbMisc.randomSleep(10L, 50L);
            // Switch the working set.
            ArrayList<MvApplyItem> temp = curr;
            curr = next;
            next = temp;
        }
        return false;
    }

}
