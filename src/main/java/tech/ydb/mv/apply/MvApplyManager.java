package tech.ydb.mv.apply;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.table.TableClient;

import tech.ydb.mv.svc.MvJobContext;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvRowFilter;
import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.support.YdbMisc;
import tech.ydb.mv.feeder.MvSink;
import tech.ydb.mv.metrics.MvMetrics;

/**
 * The apply manager processes the changes in the context of a single handler.
 * Multiple apply managers can run in a single application.
 *
 * @author zinal
 */
public class MvApplyManager implements MvSink {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyManager.class);

    private final MvActionContext context;
    private final MvApplyWorker[] workers;
    private final MvApplyQueueCounters queueCounters;
    private final MvApplyQueuePolicy queuePolicy;

    // source table name -> table apply configuration data
    private final HashMap<String, MvApply.Source> sourceConfigs = new HashMap<>();
    // target -> refresh action singleton list
    private final HashMap<MvViewExpr, MvApply.Target> targetConfigs = new HashMap<>();

    public MvApplyManager(MvJobContext jobContext) {
        this.context = new MvActionContext(jobContext, this);
        int workerCount = jobContext.getSettings().getApplyThreads();
        this.workers = new MvApplyWorker[workerCount];
        for (int i = 0; i < workerCount; ++i) {
            workers[i] = new MvApplyWorker(this, i);
        }
        this.queueCounters = new MvApplyQueueCounters();
        this.queuePolicy = new MvApplyQueuePolicy(
                jobContext.getSettings().getApplyQueueSize(),
                jobContext.getSettings().getApplyQueuePercent());
        new MvApply.Configurator(this.context)
                .build(this.sourceConfigs, this.targetConfigs);
    }

    public String getJobName() {
        return context.getHandler().getName();
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

    public int getQueueLimit() {
        return queuePolicy.getQueueLimit();
    }

    public int getQueueSize() {
        return queueCounters.getQueueSize();
    }

    public int getBatchQueueSize() {
        return queueCounters.getBatchQueueSize();
    }

    public int getMaxBatchQueueSize() {
        return queuePolicy.getMaxBatchQueue();
    }

    protected final int incrementQueueSize(boolean batch) {
        return queueCounters.increment(batch);
    }

    protected final int decrementQueueSize(int count, int batchCount) {
        return queueCounters.decrement(count, batchCount);
    }

    /**
     * Detect whether a submission should be treated as batch work.
     *
     * @param changes Change records being submitted.
     * @return {@code true} if at least one record is marked as batch.
     */
    static boolean detectBatchSubmission(Collection<MvChangeRecord> changes) {
        for (MvChangeRecord change : changes) {
            if (change.isBatch()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Normalize batch markers so the whole submission shares one flag.
     *
     * @param changes Input change records.
     * @param batch Desired batch marker for every record.
     * @return Records with the normalized batch marker.
     */
    static List<MvChangeRecord> normalizeBatchFlag(
            Collection<MvChangeRecord> changes, boolean batch) {
        ArrayList<MvChangeRecord> normalized = new ArrayList<>(changes.size());
        for (MvChangeRecord change : changes) {
            normalized.add(change.withBatch(batch));
        }
        return normalized;
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
     * Used by the controller to start the apply worker threads. No need for
     * explicit stop method here - the threads stop when the controller reports
     * itself as stopped via isRunning() method.
     */
    public void start() {
        for (MvApplyWorker w : workers) {
            w.start();
        }
        LOG.info("Started {} apply worker(s) for handler `{}`.",
                workers.length, context.getHandler().getName());
    }

    public void awaitTermination(Duration timeout) {
        if (context.isRunning()) {
            throw new IllegalStateException("Job should be stopped when "
                    + "calling awaitTermination()");
        }
        Instant waitUntil = Instant.now().plus(timeout);
        LOG.info("Waiting for shutdown of apply workers until {}", waitUntil);
        boolean running;
        do {
            running = false;
            for (MvApplyWorker w : workers) {
                if (w.isRunning()) {
                    running = true;
                    break;
                }
            }
            if (running) {
                YdbMisc.sleep(100L);
            }
            if (Instant.now().isAfter(waitUntil)) {
                break;
            }
        } while (running);
        if (running) {
            LOG.warn("Apply workers still running, moving forward with shutdown.");
        }
        LOG.info("Apply manager has been fully stopped.");
    }

    @Override
    public Collection<MvInput> getInputs() {
        return context.getHandler().getInputs().values().stream()
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
                    count, sourceTableName, context.getHandler().getName());
            return null;
        }
        return src;
    }

    private boolean doSubmit(MvApplyActionList actions, MvApply.Source sourceConfig,
            Collection<MvChangeRecord> changes, MvCommitHandler handler, boolean immediate) {
        if (actions == null) {
            actions = sourceConfig.getActions();
        }
        boolean batch = detectBatchSubmission(changes);
        List<MvChangeRecord> normalized = normalizeBatchFlag(changes, batch);
        ArrayList<MvApplyTask> curr = new ArrayList<>(normalized.size());
        for (MvChangeRecord change : normalized) {
            if (sourceConfig.getTableInfo() != change.getKey().getTableInfo()) {
                throw new IllegalArgumentException("Mixed input tables on submission");
            }
            curr.add(new MvApplyTask(change, handler, actions));
        }
        if (immediate) {
            // Forced path may overflow the queue (deadlock avoidance for
            // key-partitioned fan-out), but still updates the counters.
            curr.forEach(task -> getWorker(task, sourceConfig).submit(task));
            return true;
        }
        int position = 0;
        while (isRunning() && position < curr.size()) {
            // Admission control: interactive may use the full queue; batch is
            // capped so that applyQueuePercent stays available for interactive.
            if (queuePolicy.canAdmit(batch, getQueueSize(), getBatchQueueSize())) {
                MvApplyTask task = curr.get(position);
                getWorker(task, sourceConfig).submit(task);
                ++position;
            } else {
                // Allow the queue to get released.
                long waitMillis = ThreadLocalRandom.current().nextLong(10L, 51L);
                YdbMisc.sleep(waitMillis);
                // Report the queue wait event
                MvMetrics.recordQueueWait(context.getHandler().getName(), waitMillis);
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
    public boolean submitRefresh(MvViewExpr target,
            Collection<MvChangeRecord> changes, MvCommitHandler handler) {
        var targetConfig = targetConfigs.get(target);
        if (targetConfig == null) {
            return submitCustom(null, changes, handler);
        }
        return submitCustom(targetConfig.getRefreshActions(), changes, handler);
    }

    /**
     * Forcibly insert the input data to the queue of the proper workers.
     *
     * May overflow the expected size of the queues.
     *
     * @param changes The change records to be submitted for processing.
     * @param handler The commit processing handler
     */
    public void submitForce(Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        var sourceConfig = findSource(changes, handler);
        if (sourceConfig != null) {
            MvApplyActionList actions = sourceConfig.getActions();
            doSubmit(actions, sourceConfig, changes, handler, true);
        }
    }

    /**
     * Insert the input data to the queue of the proper workers.
     *
     * May overflow the expected size of the queues.
     *
     * @param target Perform refresh of the specified target only.
     * @param changes The change records to be submitted for processing.
     * @param handler The commit processing handler
     */
    public void submitFilter(MvViewExpr target, Collection<MvChangeRecord> changes,
            MvCommitHandler handler) {
        var sourceConfig = findSource(changes, handler);
        if (sourceConfig != null) {
            MvApplyActionList actions;
            if (target == null) {
                actions = sourceConfig.getActions();
            } else {
                var targetConfig = targetConfigs.get(target);
                if (targetConfig != null) {
                    actions = targetConfig.getRefreshActions();
                } else {
                    actions = null;
                }
            }
            doSubmit(actions, sourceConfig, changes, handler, true);
        }
    }

    public MvApplyAction createFilterAction(MvRowFilter filter) {
        if (filter == null || filter.isEmpty() || filter.getTarget() == null
                || filter.getTransformation() == null) {
            throw new IllegalArgumentException("Null or empty filter passed");
        }
        return new ActionKeysFilter(context, filter);
    }

}
