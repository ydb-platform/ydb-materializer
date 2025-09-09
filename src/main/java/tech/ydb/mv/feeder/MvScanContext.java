package tech.ydb.mv.feeder;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
class MvScanContext implements MvScanAdapter {

    private final MvHandler handler;
    private final MvTarget target;
    private final AtomicBoolean shouldRun;
    private final AtomicReference<MvKey> currentKey;
    private final AtomicReference<MvScanCommitHandler> currentHandler;
    private final Instant tvStart;

    private final MvTableInfo tableInfo;
    private final String controlTable;
    private final String sqlSelectStart;
    private final String sqlSelectNext;

    private final MvScanDao scanDao;

    public MvScanContext(MvHandler handler, MvTarget target,
            YdbConnector ydb, String controlTable) {
        this.handler = handler;
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
        this.scanDao = new MvScanDao(ydb, this);
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public void stop() {
        shouldRun.set(false);
    }

    public MvHandler getHandler() {
        return handler;
    }

    public MvTarget getTarget() {
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
    public String getHandlerName() {
        return handler.getName();
    }

    @Override
    public String getTargetName() {
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

}
