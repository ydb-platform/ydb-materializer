package tech.ydb.mv.apply;

import tech.ydb.mv.model.MvChangeRecord;
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

    private final MvApplyWorkerPool pool;
    private final int number;
    private final AtomicReference<Thread> thread = new AtomicReference<>();
    private final ArrayBlockingQueue<MvApplyTask> queue;

    public MvApplyWorker(MvApplyWorkerPool pool, int number, int queueLimit) {
        this.pool = pool;
        this.number = number;
        this.queue = new ArrayBlockingQueue<>(queueLimit);
    }

    public void start() {
        Thread t = new Thread(this);
        t.setName("ydb-mv-apply-worker-" + String.valueOf(number));
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

    public boolean submit(MvApplyTask task) {
        return queue.offer(task);
    }

    @Override
    public void run() {
        while (pool.isRunning()) {
            if ( action() <= 0 ) {
                YdbMisc.randomSleep(10L, 30L);
            }
        }
    }

    private int action() {
        // TODO: invoking actions
        return 0;
    }

}
