package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import tech.ydb.mv.MvHandlerController;
import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvInput;
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
     * @param changes The change records to be submitted for processing.
     * @param commitHandler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    public boolean submit(Collection<MvChangeRecord> changes, MvCommitHandler commitHandler) {
        if (changes.isEmpty()) {
            return true;
        }
        int count = changes.size();
        String tableName = changes.iterator().next().getKey().getTableInfo().getName();
        MvApplyConfig apply = applyConfig.get(tableName);
        if (apply==null) {
            commitHandler.apply(count);
            LOG.warn("Skipping {} records for unknown table {}", count, tableName);
            return true;
        }
        ArrayList<MvApplyTask> curr = new ArrayList<>(count);
        ArrayList<MvApplyTask> next = new ArrayList<>(count);
        for (MvChangeRecord change : changes) {
            if (! tableName.equals(change.getKey().getTableInfo().getName())) {
                throw new IllegalArgumentException("Mixed input tables on submission");
            }
            curr.add(new MvApplyTask(change, apply, commitHandler));
        }
        while (controller.isRunning()) {
            for (MvApplyTask task : curr) {
                int workerId = apply.getSelector().choose(task.getData().getKey());
                if (! workers.get(workerId).submit(task)) {
                    // add for re-processing
                    next.add(task);
                }
            }
            if (next.isEmpty()) {
                // Everything submitted
                return true;
            }
            // Switch the working set.
            curr.clear();
            ArrayList<MvApplyTask> temp = curr;
            curr = next;
            next = temp;
            // Allow the queues to get released.
            YdbMisc.randomSleep(10L, 50L);
        }
        // Exit without processing all the inputs
        return false;
    }

}
