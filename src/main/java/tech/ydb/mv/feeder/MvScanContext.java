package tech.ydb.mv.feeder;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.support.MvScanAdapter;
import tech.ydb.mv.support.MvScanDao;
import tech.ydb.mv.svc.MvJobContext;

/**
 *
 * @author zinal
 */
class MvScanContext implements MvScanAdapter {

    private final MvJobContext job;
    private final MvViewExpr target;
    private final AtomicBoolean shouldRun;
    private final AtomicReference<MvKey> currentKey;
    private final AtomicReference<MvScanCommitHandler> currentHandler;
    private final Instant tvStart;

    private final MvTableInfo tableInfo;
    private final String controlTable;
    private final String sqlSelectStart;
    private final String sqlSelectNext;

    private final MvScanDao scanDao;
    private final MvScanCompletion completion;

    public MvScanContext(MvJobContext job, MvViewExpr target, String controlTable,
            MvScanCompletion completion) {
        this.job = job;
        this.target = target;
        this.shouldRun = new AtomicBoolean(true);
        this.currentKey = new AtomicReference<>();
        this.currentHandler = new AtomicReference<>();
        this.tvStart = Instant.now();
        this.tableInfo = target.getTopMostSource().getTableInfo();
        this.controlTable = controlTable;
        try (MvSqlGen sg = new MvSqlGen(target)) {
            this.sqlSelectStart = sg.makeScanStart();
            this.sqlSelectNext = sg.makeScanNext();
        }
        this.scanDao = new MvScanDao(job.getYdb(), this);
        this.completion = completion;
    }

    public boolean isRunning() {
        return shouldRun.get() && job.isRunning();
    }

    public void stop() {
        shouldRun.set(false);
    }

    public MvJobContext getJob() {
        return job;
    }

    public MvHandler getHandler() {
        return job.getHandler();
    }

    public MvViewExpr getTarget() {
        return target;
    }

    public Instant getTvStart() {
        return tvStart;
    }

    public String getSqlSelectStart() {
        return sqlSelectStart;
    }

    public String getSqlSelectNext() {
        return sqlSelectNext;
    }

    public MvKey getCurrentKey() {
        return currentKey.get();
    }

    public void setCurrentKey(MvKey key) {
        currentKey.set(key);
    }

    public MvScanCommitHandler getCurrentHandler() {
        return currentHandler.get();
    }

    public void setCurrentHandler(MvScanCommitHandler handler) {
        currentHandler.set(handler);
    }

    public MvScanDao getScanDao() {
        return scanDao;
    }

    @Override
    public String getJobName() {
        return job.getHandler().getName();
    }

    @Override
    public String getTableName() {
        return target.getName();
    }

    @Override
    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public String getControlTable() {
        return controlTable;
    }

    public MvScanCompletion getCompletion() {
        return completion;
    }

}
