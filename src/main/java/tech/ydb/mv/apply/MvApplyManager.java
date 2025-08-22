package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.MvHandlerController;
import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.util.YdbMisc;

/**
 * The apply manager processes the changes in the context of a single handler.
 * Multiple apply managers can run in a single application.
 *
 * @author zinal
 */
public class MvApplyManager {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyManager.class);

    private final MvHandlerController controller;
    private final MvApplyWorker[] workers;

    // table name -> table apply configuration data
    private final HashMap<String, MvApplyConfig> applyConfig = new HashMap<>();

    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvApplyManager(MvHandlerController controller) {
        this.controller = controller;
        int workerCount = controller.getSettings().getApplyThreads();
        this.workers = new MvApplyWorker[workerCount];
        for (int i=0; i<workerCount; ++i) {
            workers[i] = new MvApplyWorker(this, i);
        }
        buildConfig();
    }

    private void buildConfig() {
        for (MvInput mi : controller.getMetadata().getInputs().values()) {
            MvApplyConfig mac = applyConfig.get(mi.getTableName());
            if (mac==null) {
                mac = new MvApplyConfig(mi.getTableInfo(), workers.length);
                applyConfig.put(mi.getTableName(), mac);
            }
        }
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public int getWorkersCount() {
        return workers.length;
    }

    public MvHandlerSettings getSettings() {
        return controller.getSettings();
    }

    public MvApplyWorker getWorker(int index) {
        if (index < 0) {
            index = -1 * index;
        }
        index = index % workers.length;
        return workers[index];
    }

    public void start() {
        if (shouldRun.getAndSet(true)) {
            // already running, no need to create the threads
            return;
        }
        for (MvApplyWorker w : workers) {
            w.start();
        }
    }

    public void stop() {
        shouldRun.set(false);
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
                if (! getWorker(workerId).submit(task)) {
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
