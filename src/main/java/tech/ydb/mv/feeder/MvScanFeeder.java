package tech.ydb.mv.feeder;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.apply.MvApplyActionList;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.metrics.MvMetrics;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.support.YdbMisc;
import tech.ydb.mv.svc.MvJobContext;

/**
 * Scan feeder reads the keys from the topmost-left source of a MV.
 *
 * @author zinal
 */
public class MvScanFeeder {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvScanFeeder.class);

    private final MvJobContext job;
    private final MvViewExpr target;
    private final MvKeyInfo keyInfo;
    private final AtomicReference<MvScanContext> context;
    private final MvSink sink;
    private final MvApplyActionList actions;
    private final MvScanCompletion completion;
    private final String controlTable;
    private final MvMetrics.ScanScope metricsScope;
    private final int rateLimiterLimit;
    private int rateLimiterCounter;
    private long rateLimiterStamp;

    public MvScanFeeder(
            MvJobContext job,
            MvSink sink,
            MvViewExpr target,
            MvScanSettings settings,
            MvApplyActionList actions,
            MvScanCompletion completion
    ) {
        this.job = job;
        this.target = target;
        this.keyInfo = target.getTopMostSource().getTableInfo().getKeyInfo();
        this.context = new AtomicReference<>();
        this.sink = sink;
        this.actions = actions;
        this.completion = completion;
        this.controlTable = job.getYdb().getProperty(MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        this.rateLimiterLimit = settings.getRowsPerSecondLimit();
        this.metricsScope = new MvMetrics.ScanScope(job.getFeederName(),
                target.getName(), target.getAlias());
    }

    public boolean isRunning() {
        MvScanContext ctx = context.get();
        return ctx != null && ctx.isRunning() && job.isRunning();
    }

    public synchronized boolean start() {
        if (!job.isRunning()) {
            throw new IllegalStateException("Refusing to start scan feeder "
                    + "for a stopped handler job " + job.getHandler().getName());
        }
        if (context.get() != null) {
            return false;
        }
        MvScanContext ctx = context.getAndSet(
                new MvScanContext(job.getHandler(), target, job.getYdb(), controlTable));
        if (ctx != null) {
            throw new IllegalStateException("Illegal startup sequence for MvScanFeeder");
        }
        Thread thread = new Thread(() -> safeRun());
        thread.setDaemon(true);
        thread.setName("mv-scan-feeder-"
                + job.getHandler().getName()
                + "-" + target.getName());
        thread.start();
        return true;
    }

    public synchronized boolean stop() {
        MvScanContext ctx = context.getAndSet(null);
        if (ctx == null) {
            return false;
        }
        ctx.stop();
        return true;
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
                LOG.warn("Failed scan feeder for target `{}` as {} in handler `{}` - retry pending...",
                        target.getName(), target.getAlias(), job.getHandler().getName(), ex);
            }
            sleepSome();
        }
    }

    private void run() {
        MvScanContext ctx = context.get();
        if (ctx == null) {
            LOG.error("Exiting the scanner due to missing context - PROGRAM DEFECT!");
            return;
        } else {
            MvKey key = ctx.getScanDao().initScan();
            ctx.setCurrentKey(key);
            if (key == null) {
                ctx.getScanDao().registerScan();
            }
            LOG.info("Started scan feeder for target `{}` as {} in handler `{}`, "
                    + "max rate {}, position {}", target.getName(), target.getAlias(),
                    job.getHandler().getName(), rateLimiterLimit, key);
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
        job.forgetScan(target);
        if (completion != null) {
            boolean incomplete = !isRunning();
            completion.onScanComplete(incomplete);
        }
        LOG.info("Finished scan feeder for target `{}` as {} in handler `{}`",
                target.getName(), target.getAlias(), job.getHandler().getName());
    }

    private int stepScan(MvScanContext ctx) {
        ResultSetReader rsr = null;
        MvKey key = null;
        if (ctx != null) {
            String sql;
            Params params;
            key = ctx.getCurrentKey();
            if (key == null || key.isEmpty()) {
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
        // report the metrics
        MvMetrics.recordScanSubmit(metricsScope, rsr.getRowCount());
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
            final long howmuch = fullTime - diff;
            final long period = 50L;
            diff = howmuch;
            while (isRunning() && diff > 0L) {
                YdbMisc.sleep(period);
                diff -= period;
            }
            // report the total delay in metrics
            MvMetrics.recordScanDelay(metricsScope, howmuch);
        }
    }

}
