package tech.ydb.mv.apply;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The pool of workers processing the workload.
 *
 * @author zinal
 */
public class MvApplyWorkerPool {

    private final MvApplyWorker[] workers;

    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvApplyWorkerPool(int workerCount, int queueLimit) {
        this.workers = new MvApplyWorker[workerCount];
        for (int i=0; i<workerCount; ++i) {
            workers[i] = new MvApplyWorker(this, i, queueLimit);
        }
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public int getCount() {
        return workers.length;
    }

    public MvApplyWorker get(int index) {
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

}
