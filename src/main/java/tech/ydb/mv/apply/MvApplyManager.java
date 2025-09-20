package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import tech.ydb.table.TableClient;

import tech.ydb.mv.svc.MvJobContext;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvRowFilter;
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
    private final int queueLimit;

    // source table name -> table apply configuration data
    private final HashMap<String, MvApply.Source> sourceConfigs = new HashMap<>();
    // target table name -> refresh action singleton list
    private final HashMap<String, MvApply.Target> targetConfigs = new HashMap<>();

    public MvApplyManager(MvJobContext context) {
        this.context = new MvActionContext(context, this);
        int workerCount = context.getSettings().getApplyThreads();
        this.workers = new MvApplyWorker[workerCount];
        for (int i = 0; i < workerCount; ++i) {
            workers[i] = new MvApplyWorker(this, i);
        }
        this.queueSize = new AtomicInteger(0);
        this.queueLimit = context.getSettings().getApplyQueueSize();
        new MvApply.Configurator(this.context)
                .build(this.sourceConfigs, this.targetConfigs);
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
     * @return true if any of the workers is locked by the error being retried,
     * false otherwise
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
        for (var source : sourceConfigs.values()) {
            try {
                source.getSelector().refresh(tableClient);
            } catch (Exception ex) {
                LOG.error("Table metadata refresh operation failed for {}",
                        source.getTableInfo().getName(), ex);
            }
        }
    }

    /**
     * Check the extra tasks to be started, and start them.
     */
    public void pingTasks() {
        // TODO
    }

    /**
     * Used by the controller to start the apply worker threads. No need for
     * explicit stop method here - the threads stop when the controller reports
     * itself as stopped via isRunning() method.
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

    private MvApplyWorker getWorker(MvApplyTask task, MvApply.Source src) {
        int index = src.getSelector().choose(task.getData().getKey());
        if (index < 0) {
            index = -1 * index;
        }
        index = index % workers.length;
        return workers[index];
    }

    private MvApply.Source findSource(Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        if (changes.isEmpty()) {
            return null;
        }
        String sourceTableName = changes.iterator().next().getKey().getTableName();
        var src = sourceConfigs.get(sourceTableName);
        if (src == null) {
            int count = changes.size();
            handler.commit(count);
            LOG.warn("Skipping {} changes for unexpected table `{}` in handler `{}`",
                    count, sourceTableName, context.getMetadata().getName());
            return null;
        }
        return src;
    }

    private boolean doSubmit(MvApplyActionList actions, MvApply.Source sourceConfig,
            Collection<MvChangeRecord> changes, MvCommitHandler handler, boolean immediate) {
        if (actions == null) {
            actions = sourceConfig.getActions();
        }
        int count = changes.size();
        ArrayList<MvApplyTask> curr = new ArrayList<>(count);
        for (MvChangeRecord change : changes) {
            if (sourceConfig.getTableInfo() != change.getKey().getTableInfo()) {
                throw new IllegalArgumentException("Mixed input tables on submission");
            }
            curr.add(new MvApplyTask(change, handler, actions));
        }
        if (immediate) {
            curr.forEach(task -> getWorker(task, sourceConfig).submit(task));
            return true;
        }
        int position = 0;
        while (isRunning() && position < curr.size()) {
            // backpressure condition - wait until queue space is available
            if (getQueueSize() < queueLimit) {
                MvApplyTask task = curr.get(position);
                getWorker(task, sourceConfig).submit(task);
                ++position;
            } else {
                // Allow the queue to get released.
                YdbMisc.randomSleep(10L, 50L);
            }
        }
        return (position >= curr.size());
    }

    @Override
    public boolean submit(Collection<MvChangeRecord> changes, MvCommitHandler handler) {
        var sourceConfig = findSource(changes, handler);
        if (sourceConfig == null) {
            return true;
        }
        return doSubmit(null, sourceConfig, changes, handler, false);
    }

    @Override
    public boolean submitCustom(MvApplyActionList actions,
            Collection<MvChangeRecord> changes, MvCommitHandler handler) {
        var sourceConfig = findSource(changes, handler);
        if (sourceConfig == null) {
            return true;
        }
        return doSubmit(actions, sourceConfig, changes, handler, false);
    }

    @Override
    public boolean submitRefresh(MvTarget target,
            Collection<MvChangeRecord> changes, MvCommitHandler handler) {
        var targetConfig = targetConfigs.get(target.getName());
        if (targetConfig == null) {
            return submitCustom(null, changes, handler);
        }
        return submitCustom(targetConfig.getRefreshActions(), changes, handler);
    }

    /**
     * Forcibly insert the input data to the queue of the proper workers. May
     * overflow the expected size of the queues.
     *
     * @param target Perform refresh of the specified target only. null
     * otherwise
     * @param changes The change records to be submitted for processing.
     * @param handler The commit processing handler
     */
    public void submitForce(MvTarget target, Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        var sourceConfig = findSource(changes, handler);
        if (sourceConfig != null) {
            MvApplyActionList actions;
            if (target == null) {
                actions = sourceConfig.getActions();
            } else {
                var targetConfig = targetConfigs.get(target.getName());
                if (targetConfig != null) {
                    actions = targetConfig.getRefreshActions();
                } else {
                    actions = null;
                }
            }
            doSubmit(actions, sourceConfig, changes, handler, true);
        }
    }

    public MvApplyAction createFilterAction(MvTarget target, MvRowFilter filter) {
        var targetConfig = targetConfigs.get(target.getName());
        if (targetConfig == null) {
            throw new IllegalArgumentException("Cannot produce filter action "
                    + "for unknown target " + target.getName());
        }
        if (targetConfig.getDictTrans() == null) {
            throw new IllegalArgumentException("Cannot produce filter action "
                    + "for non-dictionary target " + target.getName());
        }
        return new ActionKeysFilter(context, target, targetConfig.getDictTrans(), filter);
    }

}
