package tech.ydb.mv.feeder;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbMisc;
import tech.ydb.mv.util.YdbStruct;

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
    private final MvApplyManager applyManager;
    private final String controlTable;
    private final int rateLimiterLimit;
    private int rateLimiterCounter;
    private long rateLimiterStamp;

    public MvScanFeeder(MvJobContext job, MvApplyManager applyManager, MvTarget target) {
        this.job = job;
        this.target = target;
        this.keyInfo = target.getTopMostSource().getTableInfo().getKeyInfo();
        this.context = new AtomicReference<>();
        this.applyManager = applyManager;
        this.controlTable = job.getYdb()
                .getProperty(MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        this.rateLimiterLimit = job.getYdb().getProperty(MvConfig.CONF_SCAN_RATE, 10000);
    }

    /**
     * @return true if scan task is already registered, false otherwise
     */
    public boolean checkRegistered() {
        return ( initScan() != null );
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
        MvScanContext ctx = context.getAndSet(
                new MvScanContext(job.getMetadata(), target, job.getYdb(), controlTable));
        if (ctx != null) {
            context.set(ctx);
            return;
        }
        Thread thread = new Thread(() -> safeRun());
        thread.setDaemon(true);
        thread.setName("ydb-scan-feeder-"
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
        stop();
        unregisterScan();
    }

    private void safeRun() {
        while (true) {
            try {
                run();
                return;
            } catch (Exception ex) {
                LOG.info("Failed scan feeder for target {} in handler {} - retry pending...",
                        target.getName(), job.getMetadata().getName(), ex);
            }
            YdbMisc.randomSleep(2000L, 20000L);
        }
    }

    private void run() {
        MvScanContext ctx = context.get();
        if (ctx==null) {
            LOG.error("Exiting the scanner due to missing context - PROGRAM DEFECT!");
            return;
        } else {
            MvKey key = initScan();
            ctx.setCurrentKey(key);
            if (key == null) {
                registerStartScan();
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
                output.add(new MvChangeRecord(key));
            }
            ctx.setCurrentKey(key);
            handler = new MvScanCommitHandler(ctx,
                    key, output.size(), ctx.getCurrentHandler(), false);
            applyManager.submit(output, handler);
        } else {
            handler = new MvScanCommitHandler(ctx,
                    key, 0, ctx.getCurrentHandler(), true);
        }
        ctx.setCurrentHandler(handler);
        // apply check for the case when the final commit is already performed
        handler.apply(0);
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

    private MvKey initScan() {
        MvScanContext ctx = context.get();
        String sql = ctx.getSqlPosSelect();
        Params params = Params.of(
                "$handler_name", ctx.getHandlerName(),
                "$table_name", ctx.getTargetName()
        );
        ResultSetReader rsr = job.getYdb().sqlRead(sql, params).getResultSet(0);
        MvKey key = null;
        if (rsr.next()) {
            YdbStruct ys = YdbStruct.fromJson(rsr.getColumn(0).getText());
            key = new MvKey(ys, target.getTableInfo());
        }
        return key;
    }

    private void registerStartScan() {
        MvScanContext ctx = context.get();
        String sql = ctx.getSqlPosUpsert();
        Params params = Params.of(
                "$handler_name", ctx.getHandlerName(),
                "$table_name", ctx.getTargetName(),
                "$key_position", PrimitiveValue.newText("{}")
        );
        job.getYdb().sqlWrite(sql, params);
    }

    private void unregisterScan() {
        String sql = "DECLARE $handler_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "DELETE FROM `" + controlTable
                + "` WHERE handler_name=$handler_name "
                + "   AND table_name=$table_name";
        Params params = Params.of(
                "$handler_name", PrimitiveValue.newText(job.getMetadata().getName()),
                "$table_name", PrimitiveValue.newText(target.getName())
        );
        job.getYdb().sqlWrite(sql, params);
    }

}
