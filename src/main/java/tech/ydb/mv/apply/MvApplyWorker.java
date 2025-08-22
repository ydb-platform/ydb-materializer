package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

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
    private final ArrayBlockingQueue<MvApplyTask> queue;
    private final ArrayList<MvApplyTask> activeTasks;

    public MvApplyWorker(MvApplyManager owner, int number) {
        this.owner = owner;
        this.workerNumber = number;
        this.activeTasks = new ArrayList<>(10);
        this.queue = new ArrayBlockingQueue<>(owner.getSettings().getApplyQueueSize());
    }

    public void start() {
        Thread t = new Thread(this);
        t.setName("ydb-mv-apply-worker-" + String.valueOf(workerNumber));
        t.setDaemon(true);
        Thread old = thread.getAndSet(t);
        if (old!=null && old.isAlive()) {
            thread.set(old);
        } else {
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

    /**
     * Attempt to add the task to the input queue.
     * The input queue may be full, in which case the tasks cannot be added
     * until some space is released in the queue.
     *
     * @param task The task to be added
     * @return true, if the task was added, and false otherwise.
     */
    public boolean submit(MvApplyTask task) {
        return queue.offer(task);
    }

    @Override
    public void run() {
        while (owner.isRunning()) {
            if ( action() <= 0 ) {
                // nothing has been done, so make some sleep
                YdbMisc.randomSleep(10L, 30L);
            }
        }
    }

    private int action() {
        queue.drainTo(activeTasks);
        if (activeTasks.isEmpty()) {
            return 0;
        }
        PerAction retries = new PerAction().apply();
        if (!retries.isEmpty()) {
            processRetries(retries);
        }
        new PerCommit().apply();
        return activeTasks.size();
    }

    private void processRetries(PerAction retries) {
        int retryNumber = 0;
        while (! retries.isEmpty()) {
            
        }
    }

    private void applyAction(MvApplyAction action, List<MvApplyTask> tasks, PerAction retries) {
        try {
            action.apply(tasks);
        } catch(Exception ex) {
            retries.items.put(action, tasks);
            LOG.warn("Action execution failed, scheduling for retry", ex);
        }
    }

    private class PerAction {
        final HashMap<MvApplyAction, List<MvApplyTask>> items = new HashMap<>();

        PerAction() {
            for (MvApplyTask task : activeTasks) {
                task.clearErrors();
                for (MvApplyAction action : task.getActions().getActions()) {
                    List<MvApplyTask> tasks = items.get(action);
                    if (tasks==null) {
                        tasks = new ArrayList<>();
                        items.put(action, tasks);
                    }
                    tasks.add(task);
                }
            }
        }

        boolean isEmpty() {
            return items.isEmpty();
        }

        PerAction apply() {
            PerAction retries = new PerAction();
            items.forEach((action, tasks) -> applyAction(action, tasks, retries));
            return retries;
        }
    }

    private class PerCommit {
        final HashMap<MvCommitHandler, Integer> items = new HashMap<>();

        PerCommit() {
            for (MvApplyTask task : activeTasks) {
                if (task.getErrorCount() > 0) {
                    // Skip commits for error records.
                    continue;
                }
                Integer numTasks = items.get(task.getCommit());
                if (numTasks==null) {
                    items.put(task.getCommit(), 1);
                } else {
                    items.put(task.getCommit(), 1 + numTasks);
                }
            }
        }

        void apply() {
            items.forEach((h, n) -> h.apply(n));
        }
    }

}
