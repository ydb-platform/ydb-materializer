package tech.ydb.mv.apply;

import java.util.concurrent.atomic.AtomicBoolean;
import tech.ydb.mv.model.MvKey;

/**
 * The collection of workers, each having its own input queue to process
 * the updates to the MVs being supported by the current application.
 *
 * @author zinal
 */
public class MvApplyManager {

    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public boolean isRunning() {
        return shouldRun.get();
    }

    /**
     * Start the worker threads.
     */
    public void start() {

    }

    /**
     * Stop the worker threads.
     */
    public void stop() {

    }

    /**
     * Insert the key to the input queue of the proper worker.
     * In case the queue is full, wait until the capacity becomes available,
     * or until the update manager is stopped.
     *
     * In case there is no proper queue for the key, or in case the update
     * manager has been stopped, false is returned.
     *
     * @param key The key to be put to the queue.
     * @return true, if the key has got to the queue, and false otherwise.
     */
    public boolean submit(MvKey key) {
        return false;
    }

}
