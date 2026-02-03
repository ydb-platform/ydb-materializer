package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.support.YdbMisc;
import tech.ydb.mv.metrics.MvMetrics;

/**
 * The apply worker is an active object (thread) with the input queue to
 * process. It handles the changes from each MV being handled on the current
 * application instance.
 *
 * @author zinal
 */
class MvApplyWorker implements Runnable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyWorker.class);

    private final MvApplyManager owner;
    private final int workerNumber;
    private final AtomicReference<Thread> thread = new AtomicReference<>();
    private final LinkedBlockingQueue<MvApplyTask> queue;
    private final AtomicBoolean locked = new AtomicBoolean(false);

    public MvApplyWorker(MvApplyManager owner, int number) {
        this.owner = owner;
        this.workerNumber = number;
        this.queue = new LinkedBlockingQueue<>();
    }

    public void start() {
        Thread t = new Thread(this);
        t.setName("mv-apply-worker-" + owner.getJobName()
                + "-" + String.valueOf(workerNumber));
        t.setDaemon(false);
        Thread old = thread.getAndSet(t);
        if (old != null && old.isAlive()) {
            thread.set(old);
        } else {
            locked.set(false);
            t.start();
        }
    }

    public boolean isRunning() {
        Thread t = thread.get();
        if (t == null) {
            return false;
        }
        return t.isAlive();
    }

    public boolean isLocked() {
        return locked.get();
    }

    /**
     * Always adds the task to the queue. May overflow the expected size.
     *
     * @param task The task to be added
     */
    public void submit(MvApplyTask task) {
        owner.incrementQueueSize();
        queue.add(task);
        LOG.debug("Task accepted: {}", task);
    }

    @Override
    public void run() {
        while (owner.isRunning()) {
            if (action() == 0) {
                // nothing has been done, so make some sleep
                YdbMisc.randomSleep(10L, 30L);
            }
        }
    }

    private int action() {
        ArrayList<MvApplyTask> activeTasks = new ArrayList<>();
        queue.drainTo(activeTasks);
        if (activeTasks.isEmpty()) {
            return 0;
        }
        owner.decrementQueueSize(activeTasks.size());
        PerAction retries = new PerAction().addItems(activeTasks).apply();
        if (!processRetries(retries)) {
            // No commit unless no retries needed, or retries succeeded.
            // Tasks being retried are lost here, but that's not a problem,
            // because the worker is shutting down anyway. The tasks will be
            // re-processed after the next startup.
            return -1;
        }
        new PerCommit(activeTasks).apply();
        return activeTasks.size();
    }

    private boolean processRetries(PerAction retries) {
        if (retries.isEmpty()) {
            return true;
        }
        // we have retries pending, so switch to locked mode
        locked.set(true);
        int retryNumber = 0;
        while (!retries.isEmpty()) {
            if (!makeDelay(retryNumber)) {
                return false;
            }
            retries = retries.apply();
            retryNumber += 1;
        }
        // retries succeeded, we unlock and move forward
        locked.set(false);
        return true;
    }

    private boolean makeDelay(int retryNumber) {
        long sleepTime = 250 << Math.min(retryNumber, 8);
        long tvFinish = System.currentTimeMillis() + sleepTime;
        do {
            if (!owner.isRunning()) {
                return false;
            }
            YdbMisc.sleep(50L);
        } while (tvFinish > System.currentTimeMillis());
        return true;
    }

    private void applyAction(MvApplyAction action, List<MvApplyTask> tasks, PerAction retries) {
        ActionBase base = (action instanceof ActionBase) ? (ActionBase) action : null;
        String type = (base == null) ? null : base.getMetricsType();
        String target = (base == null) ? null : base.getMetricsTarget();
        String alias = (base == null) ? null : base.getMetricsAlias();
        String source = (base == null) ? null : base.getMetricsSource();
        String item = (base == null) ? null : base.getMetricsItem();
        long startNs = System.nanoTime();
        try {
            action.apply(tasks);
            MvMetrics.recordProcessedCount(type, target, alias, source, item, tasks.size());
        } catch (Exception ex) {
            retries.addItems(tasks, action);
            MvMetrics.recordProcessingError(type, target, alias, source, item, tasks.size());
            String lastSql = ActionBase.getLastSqlStatement();
            if (lastSql != null) {
                LOG.error("Execution failed for action {}, scheduling for retry {} tasks."
                        + "Last SQL:\n{}\n", action, lastSql, tasks.size(), ex);
            } else {
                LOG.error("Execution failed for action {}, scheduling for retry {} tasks",
                        action, tasks.size(), ex);
            }
        } finally {
            long durationNs = System.nanoTime() - startNs;
            MvMetrics.recordProcessingTime(type, target, alias, source, item, durationNs);
        }
    }

    private class PerAction {

        final HashMap<MvApplyAction, List<MvApplyTask>> items = new HashMap<>();

        boolean isEmpty() {
            return items.isEmpty();
        }

        PerAction addItems(List<MvApplyTask> input) {
            for (MvApplyTask task : input) {
                for (MvApplyAction action : task.getActions()) {
                    addItem(task, action);
                }
            }
            return this;
        }

        PerAction addItems(List<MvApplyTask> input, MvApplyAction cur) {
            if (cur == null) {
                return addItems(input);
            }
            for (MvApplyTask task : input) {
                addItem(task, cur);
            }
            return this;
        }

        PerAction addItem(MvApplyTask task, MvApplyAction action) {
            List<MvApplyTask> tasks = items.get(action);
            if (tasks == null) {
                tasks = new ArrayList<>();
                items.put(action, tasks);
            }
            tasks.add(task);
            return this;
        }

        PerAction apply() {
            PerAction retries = new PerAction();
            items.forEach((action, tasks) -> applyAction(action, tasks, retries));
            return retries;
        }
    }

    private class PerCommit {

        final HashMap<MvCommitHandler, Integer> items = new HashMap<>();

        PerCommit(ArrayList<MvApplyTask> input) {
            for (MvApplyTask task : input) {
                Integer numTasks = items.get(task.getCommit());
                if (numTasks == null) {
                    items.put(task.getCommit(), 1);
                } else {
                    items.put(task.getCommit(), 1 + numTasks);
                }
            }
        }

        void apply() {
            items.forEach((h, n) -> h.commit(n));
        }
    }

}
