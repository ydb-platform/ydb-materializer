package tech.ydb.mv.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.StructType;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public class MvUpdater {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvUpdater.class);

    private final YdbConnector conn;
    private final MvTarget target;
    private final String upsertSql;
    private final StructType keyType;
    private final String tablePath;
    private final int nthreads;
    private final ArrayList<Worker> workers = new ArrayList<>();
    private final ArrayBlockingQueue<QueueItem> queue;
    private volatile FlowControl flowControl = null;

    public MvUpdater(YdbConnector conn, MvTarget target, int threads) {
        this.conn = conn;
        this.target = target;
        this.nthreads = (threads > 0) ? threads : 1;
        try (SqlGen sg = new SqlGen(target)) {
            this.upsertSql = sg.makeUpsert();
            this.keyType = sg.getKeyType();
            this.tablePath = conn.fullTableName(sg.getMainTable());
        }
        this.queue = new ArrayBlockingQueue<>(nthreads * 10);
    }

    public YdbConnector getConn() {
        return conn;
    }

    public MvTarget getTarget() {
        return target;
    }

    public String getUpsertSql() {
        return upsertSql;
    }

    public StructType getKeyType() {
        return keyType;
    }

    public List<String> getInputKeyColumns() {
        return target.getInputKeyColumns();
    }

    public String getTablePath() {
        return tablePath;
    }

    public FlowControl getFlowControl() {
        return flowControl;
    }

    public void setFlowControl(FlowControl flowControl) {
        this.flowControl = flowControl;
    }

    public int getRemainingQueueCapacity() {
        return queue.remainingCapacity();
    }

    public boolean isRunning() {
        return true;
    }

    public synchronized void start() {
        if (! workers.isEmpty()) {
            return;
        }
        for (int i=0; i<nthreads; ++i) {
            workers.add(new Worker(i));
        }
        for (Worker w : workers) {
            w.thread.start();
        }
    }

    public boolean addMessage(ResultSetReader batch, CommitAction action) {
        QueueItem qi = new QueueItem(batch, action);
        while (isRunning()) {
            try {
                if ( queue.offer(qi, 50L, TimeUnit.MILLISECONDS) ) {
                    return true;
                }
            } catch(InterruptedException ix) {}
        }
        return false; // failing due to shutdown
    }

    private void shortSleep() {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(30L, 70L));
        } catch(InterruptedException ix) {}
    }

    private void requestMessages(int count) {
        if (flowControl != null) {
            flowControl.requestMessages(count);
        }
    }

    private void process(ArrayList<QueueItem> items) {

    }

    private void commit(ArrayList<QueueItem> items) {
        for (QueueItem qi : items) {
            qi.action.apply();
        }
    }

    private class Worker implements Runnable {
        final Thread thread;
        final ArrayList<QueueItem> items = new ArrayList<>();
        boolean showErrors = true;

        Worker(int num) {
            this.thread = new Thread(this, "MvUpdater-" + target.getName()
                    + "-" + String.valueOf(num));
            this.thread.setDaemon(true);
            this.thread.setPriority(Thread.NORM_PRIORITY);
        }

        @Override
        public void run() {
            while (isRunning()) {
                if (Thread.interrupted()) {
                    break;
                }
                // avoid getting more work when we have enough
                int szPrev = items.size();
                if (szPrev < 10000) {
                    queue.drainTo(items);
                }
                requestMessages(items.size() - szPrev);
                if (items.isEmpty()) {
                    shortSleep();
                } else {
                    handleItems();
                }
            }
        }

        private void handleItems() {
            try {
                process(items);
                commit(items);
                items.clear(); // clear on success
                if (!showErrors) {
                    showErrors = true;
                    LOG.info("Error logging resumed");
                }
            } catch(Exception ex) {
                if (showErrors) {
                    showErrors = false;
                    LOG.error("Failed to process the updates on MV {}, error logging suspended",
                            target.getName(), ex);
                }
                shortSleep();
            }
        }
    }

    private static class QueueItem {
        final Object batch;
        final CommitAction action;

        public QueueItem(ResultSetReader batch, CommitAction action) {
            this.batch = batch;
            this.action = action;
        }

        public boolean isResultSet() {
            return (batch instanceof ResultSetReader);
        }

        public ResultSetReader toResultSet() {
            return (ResultSetReader) batch;
        }
    }

    public static interface CommitAction {
        void apply();
    }

    public static interface FlowControl {
        void requestMessages(int count);
    }

}
