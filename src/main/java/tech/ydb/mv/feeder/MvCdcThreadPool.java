package tech.ydb.mv.feeder;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Набор потоков для обработки сообщений CDC.
 * Общий на весь процесс.
 *
 * @author zinal
 */
public class MvCdcThreadPool {

    private final int size;
    private final Executor executor;

    public MvCdcThreadPool(int size) {
        this.size = size;
        this.executor = Executors.newFixedThreadPool(size, new Factory());
    }

    public int getSize() {
        return size;
    }

    public Executor getExecutor() {
        return executor;
    }

    private static class Factory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(Thread.currentThread().getThreadGroup(), r,
                                  "ydb-cdc-worker-" + threadNumber.getAndIncrement(),
                                  0);
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

}
