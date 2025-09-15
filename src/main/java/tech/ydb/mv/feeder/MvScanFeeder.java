package tech.ydb.mv.feeder;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.apply.MvApplyActionList;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.support.YdbMisc;

/**
 * Scan feeder reads the keys from the topmost-left source of a MV.
 *
 * @author zinal
 */
public class MvScanFeeder {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvScanFeeder.class);

    private final MvJobContext job;
    private final MvTarget target;
    private final MvKeyInfo keyInfo;
    private final AtomicReference<MvScanContext> context;
    private final MvCdcSink sink;
    private final MvApplyActionList actions;
    private final String controlTable;
    private final int rateLimiterLimit;
    private int rateLimiterCounter;
    private long rateLimiterStamp;

    public MvScanFeeder(MvJobContext job, MvCdcSink sink,
            MvTarget target, MvScanSettings settings, MvApplyActionList actions) {
        this.job = job;
        this.target = target;
        this.keyInfo = target.getTopMostSource().getTableInfo().getKeyInfo();
        this.context = new AtomicReference<>();
        this.sink = sink;
        this.actions = actions;
        this.controlTable = YdbConnector.safe(settings.getControlTableName());
        this.rateLimiterLimit = settings.getRowsPerSecondLimit();
    }

    public boolean isRunning() {
        MvScanContext ctx = context.get();
        return ctx != null && ctx.isRunning() && job.isRunning();
    }

    public synchronized void start() {
        if (!job.isRunning()) {
            throw new IllegalStateException("Refusing to start scan feeder "
                    + "for a stopped handler job " + job.getMetadata().getName());
        }
        if (context.get() != null) {
            return;
        }
        MvScanContext ctx = context.getAndSet(
                new MvScanContext(job.getMetadata(), target, job.getYdb(), controlTable));
        if (ctx != null) {
            context.set(ctx);
            return;
        }
        Thread thread = new Thread(() -> safeRun());
        thread.setDaemon(true);
        thread.setName("mv-scan-feeder-"
                + job.getMetadata().getName()
                + "-" + target.getName());
        thread.start();
    }

    public synchronized void stop() {
        MvScanContext ctx = context.getAndSet(null);
        if (ctx != null) {
            ctx.stop();
        }
    }

    public synchronized void stopAndUnregister() {
        MvScanContext ctx = context.getAndSet(null);
        if (ctx != null) {
            ctx.stop();
            ctx.getScanDao().unregisterScan();
        }
    }

    private void sleepSome() {
        final long tvFinish = System.currentTimeMillis()
                + ThreadLocalRandom.current().nextLong(2000, 20000);
        while (isRunning()) {
            YdbMisc.sleep(100L);
            if (System.currentTimeMillis() >= tvFinish) {
                return;
            }
        }
    }

    private void safeRun() {
        while (isRunning()) {
            try {
                run();
                return;
            } catch (Exception ex) {
                LOG.info("Failed scan feeder for target {} in handler {} - retry pending...",
                        target.getName(), job.getMetadata().getName(), ex);
            }
            sleepSome();
        }
    }

    private void run() {
        MvScanContext ctx = context.get();
        if (ctx==null) {
            LOG.error("Exiting the scanner due to missing context - PROGRAM DEFECT!");
            return;
        } else {
            MvKey key = ctx.getScanDao().initScan();
            ctx.setCurrentKey(key);
            if (key == null) {
                ctx.getScanDao().registerScan();
            }
            LOG.info("Started scan feeder for target {} in handler {}, position {}",
                    target.getName(), job.getMetadata().getName(), key);
        }
        rateLimiterCounter = 0;
        rateLimiterStamp = System.currentTimeMillis();
        while (isRunning()) {
            int count = stepScan(ctx);
            if (count <= 0) {
                break;
            }
            rateLimiter(count);
        }
        ctx.getScanDao().unregisterScan();
        job.completeScan(target);
        LOG.info("Finished scan feeder for target {} in handler {}",
                target.getName(), job.getMetadata().getName());
    }

    private int stepScan(MvScanContext ctx) {
        ResultSetReader rsr = null;
        MvKey key = null;
        if (ctx != null) {
            String sql;
            Params params;
            key = ctx.getCurrentKey();
            if (key==null || key.isEmpty()) {
                sql = ctx.getSqlSelectStart();
                params = Params.of("$limit", PrimitiveValue.newUint64(1000L));
            } else {
                sql = ctx.getSqlSelectNext();
                params = Params.create();
                params.put("$limit", PrimitiveValue.newUint64(1000L));
                int index = 0;
                for (String name : key.getTableInfo().getKey()) {
                    params.put("$c" + String.valueOf(index + 1), key.convertValue(index));
                    ++index;
                }
            }
            rsr = job.getYdb().sqlRead(sql, params).getResultSet(0);
        }
        if (rsr != null) {
            processScanResult(ctx, key, rsr);
            return rsr.getRowCount();
        }
        return 0;
    }

    private void processScanResult(MvScanContext ctx, MvKey key, ResultSetReader rsr) {
        MvScanCommitHandler handler;
        if (rsr.getRowCount() > 0) {
            ArrayList<MvChangeRecord> output = new ArrayList<>();
            while (rsr.next()) {
                key = new MvKey(rsr, keyInfo);
                output.add(new MvChangeRecord(key, ctx.getTvStart()));
            }
            ctx.setCurrentKey(key);
            handler = new MvScanCommitHandler(ctx,
                    key, output.size(), ctx.getCurrentHandler(), false);
            if (actions != null) {
                sink.submitCustom(actions, output, handler);
            } else {
                sink.submitRefresh(target, output, handler);
            }
        } else {
            handler = new MvScanCommitHandler(ctx,
                    key, 0, ctx.getCurrentHandler(), true);
        }
        ctx.setCurrentHandler(handler);
        // apply check for the case when the final commit is already performed
        handler.commit(0);
    }

    private void rateLimiter(int count) {
        rateLimiterCounter += count;
        if (rateLimiterCounter < rateLimiterLimit) {
            return;
        }
        long tv = System.currentTimeMillis();
        long diff = tv - rateLimiterStamp;
        long fullTime = ((long) rateLimiterCounter) * 1000L / ((long) rateLimiterLimit);
        rateLimiterCounter = 0;
        rateLimiterStamp = tv;
        if (fullTime > diff) {
            diff = fullTime - diff;
            long period = 50L;
            while (isRunning() && diff > 0L) {
                YdbMisc.sleep(period);
                diff -= period;
            }
        }
    }

}
