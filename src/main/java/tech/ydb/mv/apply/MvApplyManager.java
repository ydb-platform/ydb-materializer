package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ydb.table.TableClient;

import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.feeder.MvCdcSink;
import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.support.YdbMisc;

/**
 * The apply manager processes the changes in the context of a single handler.
 * Multiple apply managers can run in a single application.
 *
 * @author zinal
 */
public class MvApplyManager implements MvCdcSink {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyManager.class);

    private final MvActionContext context;
    private final MvApplyWorker[] workers;
    private final AtomicInteger queueSize;

    // source table name -> table apply configuration data
    private final HashMap<String, MvApplyConfig> sourceActions = new HashMap<>();
    // target table name -> refresh action singleton list
    private final HashMap<String, MvApplyActionList> refreshActions = new HashMap<>();

    public MvApplyManager(MvJobContext context) {
        this.context = new MvActionContext(context, this);
        int workerCount = context.getSettings().getApplyThreads();
        this.workers = new MvApplyWorker[workerCount];
        for (int i=0; i<workerCount; ++i) {
            workers[i] = new MvApplyWorker(this, i);
        }
        this.queueSize = new AtomicInteger(0);
        new MvApplyConfig.Configurator(this.context)
                .build(this.sourceActions, this.refreshActions);
    }

    public String getJobName() {
        return context.getMetadata().getName();
    }

    public boolean isRunning() {
        return context.isRunning();
    }

    public int getWorkersCount() {
        return workers.length;
    }

    public MvHandlerSettings getSettings() {
        return context.getSettings();
    }

    public int getQueueSize() {
        return queueSize.get();
    }

    protected final int incrementQueueSize() {
        return queueSize.incrementAndGet();
    }

    protected final int decrementQueueSize(int count) {
        int temp = queueSize.addAndGet(-1 * count);
        if (temp < 0) {
            LOG.warn("Queue size below zero: {}", temp);
            queueSize.set(0);
            return 0;
        }
        return temp;
    }

    /**
     * @return The number of workers locked in retry logic (so not progressing)
     */
    public int getLockedWorkersCount() {
        int count = 0;
        for (MvApplyWorker w : workers) {
            if (w.isLocked()) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * @return true if any of the workers is locked by the error being retried, false otherwise
     */
    public boolean isLocked() {
        return (getLockedWorkersCount() > 0);
    }

    /**
     * Refresh the worker selector setup by reading the fresh partitioning data.
     *
     * @param tableClient Table client is needed to describe the source tables.
     */
    public void refreshSelectors(TableClient tableClient) {
        sourceActions.values().forEach(ac -> ac.getSelector().refresh(tableClient));
    }

    /**
     * Used by the controller to start the apply worker threads.
     * No need for explicit stop method here - the threads stop when
     * the controller reports itself as stopped via isRunning() method.
     */
    public void start() {
        for (MvApplyWorker w : workers) {
            w.start();
        }
        LOG.info("Started {} apply worker(s) for handler `{}`.",
                workers.length, context.getMetadata().getName());
    }

    @Override
    public Collection<MvInput> getInputs() {
        return context.getMetadata().getInputs().values().stream()
                .filter(mi -> !mi.isBatchMode())
                .toList();
    }

    private MvApplyWorker getWorker(int index) {
        if (index < 0) {
            index = -1 * index;
        }
        index = index % workers.length;
        return workers[index];
    }

    private ArrayList<MvApplyTask> convertChanges(
            MvTarget target,
            Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        int count = changes.size();
        ArrayList<MvApplyTask> curr = new ArrayList<>(count);
        String tableName = changes.iterator().next().getKey().getTableName();
        MvApplyConfig apply = sourceActions.get(tableName);
        if (apply == null) {
            handler.commit(count);
            LOG.warn("Skipping {} change records for unexpected table `{}` in handler `{}`",
                    count, tableName, context.getMetadata().getName());
            return curr;
        }
        MvApplyActionList actions = null;
        if (target != null) {
            actions = refreshActions.get(target.getName());
        }
        if (actions == null) {
            actions = apply.getActions();
        }
        for (MvChangeRecord change : changes) {
            if (!tableName.equals(change.getKey().getTableName())) {
                throw new IllegalArgumentException("Mixed input tables on submission");
            }
            int workerId = apply.getSelector().choose(change.getKey());
            curr.add(new MvApplyTask(change, handler, actions, workerId));
        }
        return curr;
    }

    @Override
    public boolean submit(MvTarget target, Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        if (changes.isEmpty()) {
            return true;
        }

        ArrayList<MvApplyTask> curr = convertChanges(target, changes, handler);
        if (curr.isEmpty()) {
            return true; // fast exit
        }
        ArrayList<MvApplyTask> next = new ArrayList<>();
        while (isRunning()) {
            for (MvApplyTask task : curr) {
                if (! getWorker(task.getWorkerId()).submit(task)) {
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

    @Override
    public void submitForce(MvTarget target, Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        convertChanges(target, changes, handler).forEach(
                task -> getWorker(task.getWorkerId()).submitForce(task)
        );
    }

}
