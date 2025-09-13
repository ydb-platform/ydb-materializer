package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.util.YdbMisc;

/**
 * The apply worker is an active object (thread) with the input queue to process.
 * It handles the changes from each MV being handled on the current application instance.
 *
 * @author zinal
 */
public class MvApplyWorker implements Runnable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyWorker.class);

    private final MvApplyManager owner;
    private final int workerNumber;
    private final AtomicReference<Thread> thread = new AtomicReference<>();
    private final LinkedBlockingQueue<MvApplyTask> queue;
    private final int queueLimit;
    private final ArrayList<MvApplyTask> activeTasks;
    private final AtomicBoolean locked = new AtomicBoolean(false);

    public MvApplyWorker(MvApplyManager owner, int number) {
        this.owner = owner;
        this.workerNumber = number;
        this.activeTasks = new ArrayList<>(10);
        this.queue = new LinkedBlockingQueue<>();
        this.queueLimit = owner.getSettings().getApplyQueueSize();
    }

    public void start() {
        Thread t = new Thread(this);
        t.setName("ydb-mv-apply-worker-" + String.valueOf(workerNumber));
        t.setDaemon(false);
        Thread old = thread.getAndSet(t);
        if (old!=null && old.isAlive()) {
            thread.set(old);
        } else {
            locked.set(false);
            t.start();
        }
    }

    public boolean isRunning() {
        Thread t = thread.get();
        if (t==null) {
            return false;
        }
        return t.isAlive();
    }

    public boolean isLocked() {
        return locked.get();
    }

    /**
     * Attempt to add the task to the input queue.
     * The input queue may be full, in which case the tasks cannot be added
     * until some space is released in the queue.
     * May sometimes overflow the queue, up to the number of concurrent
     * calls to the method when the queue is mostly-full.
     *
     * @param task The task to be added
     * @return true, if the task was added, and false otherwise.
     */
    public boolean submit(MvApplyTask task) {
        if (queueLimit < owner.getQueueSize()) {
            return false;
        }
        submitForce(task);
        return true;
    }

    /**
     * Always adds the task to the queue.
     * May overflow the expected size.
     *
     * @param task The task to be added
     */
    public void submitForce(MvApplyTask task) {
        owner.incrementQueueSize();
        queue.add(task);
    }

    @Override
    public void run() {
        while (owner.isRunning()) {
            if ( action() == 0 ) {
                // nothing has been done, so make some sleep
                YdbMisc.randomSleep(10L, 30L);
            }
        }
    }

    private int action() {
        activeTasks.clear();
        queue.drainTo(activeTasks);
        if (activeTasks.isEmpty()) {
            return 0;
        }
        owner.decrementQueueSize(activeTasks.size());
        PerAction retries = new PerAction().addItems(activeTasks).apply();
        if (! processRetries(retries)) {
            // no commit unless no retries needed, or retries succeeded
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
        while (! retries.isEmpty()) {
            if (! makeDelay(retryNumber)) {
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
            if (! owner.isRunning()) {
                return false;
            }
            YdbMisc.sleep(50L);
        } while (tvFinish > System.currentTimeMillis());
        return true;
    }

    private void applyAction(MvApplyAction action, List<MvApplyTask> tasks, PerAction retries) {
        try {
            action.apply(tasks);
        } catch(Exception ex) {
            retries.addItems(tasks, action);
            String lastSql = ActionBase.getLastSqlStatement();
            if (lastSql!=null) {
                LOG.error("Execution failed for action {}, scheduling for retry. Last SQL:\n{}\n",
                        action, lastSql, ex);
            } else {
                LOG.error("Execution failed for action {}, scheduling for retry",
                        action, ex);
            }
        }
    }

    private class PerAction {
        final HashMap<MvApplyAction, List<MvApplyTask>> items = new HashMap<>();

        boolean isEmpty() {
            return items.isEmpty();
        }

        PerAction addItems(List<MvApplyTask> input) {
            for (MvApplyTask task : input) {
                for (MvApplyAction action : task.getActions().getActions()) {
                    addItem(task, action);
                }
            }
            return this;
        }

        PerAction addItems(List<MvApplyTask> input, MvApplyAction cur) {
            if (cur==null) {
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
                if (numTasks==null) {
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
