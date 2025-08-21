package tech.ydb.mv.apply;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

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
    private final ArrayBlockingQueue<MvChangeRecord> queue;

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

    public boolean submit(MvChangeRecord item) {
        return queue.offer(item);
    }

    @Override
    public void run() {
        while (pool.isRunning()) {
            action();
        }
    }

    private void action() {

    }

}
