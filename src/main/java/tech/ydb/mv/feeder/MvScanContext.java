package tech.ydb.mv.feeder;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public class MvScanContext {

    private final MvHandler handler;
    private final MvTarget target;
    private final SessionRetryContext retryCtx;
    private final AtomicBoolean shouldRun;
    private final AtomicReference<MvKey> currentKey;
    private final AtomicReference<MvScanCommitHandler> currentHandler;

    private final String sqlPosUpsert;
    private final String sqlPosDelete;
    private final String sqlPosSelect;
    private final String sqlSelectStart;
    private final String sqlSelectNext;

    private final Value<?> handlerName;
    private final Value<?> targetName;

    public MvScanContext(MvHandler handler, MvTarget target,
            YdbConnector ydb, String controlTable) {
        this.handler = handler;
        this.target = target;
        this.retryCtx = ydb.getQueryRetryCtx();
        this.shouldRun = new AtomicBoolean(true);
        this.currentKey = new AtomicReference<>();
        this.currentHandler = new AtomicReference<>();
        this.sqlPosUpsert = makePosUpsert(controlTable);
        this.sqlPosDelete = makePosDelete(controlTable);
        this.sqlPosSelect = makePosSelect(controlTable);
        try (MvSqlGen sg = new MvSqlGen(target)) {
            this.sqlSelectStart = sg.makeScanStart();
            this.sqlSelectNext = sg.makeScanNext();
        }
        this.handlerName = PrimitiveValue.newText(handler.getName());
        this.targetName = PrimitiveValue.newText(target.getName());
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

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public String getSqlPosUpsert() {
        return sqlPosUpsert;
    }

    public String getSqlPosDelete() {
        return sqlPosDelete;
    }

    public String getSqlPosSelect() {
        return sqlPosSelect;
    }

    public String getSqlSelectStart() {
        return sqlSelectStart;
    }

    public String getSqlSelectNext() {
        return sqlSelectNext;
    }

    public Value<?> getHandlerName() {
        return handlerName;
    }

    public Value<?> getTargetName() {
        return targetName;
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

    private static String makePosUpsert(String controlTable) {
        return "DECLARE $handler_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "DECLARE $key_position AS JsonDocument; "
                + "UPSERT INTO `" + controlTable + "` "
                + "(handler_name, table_name, updated_at, key_position) "
                + "VALUES ($handler_name, $table_name, CurrentUtcTimestamp(), $key_position);";
    }

    private static String makePosDelete(String controlTable) {
        return "DECLARE $handler_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "DELETE FROM `" + controlTable + "` "
                + "WHERE handler_name=$handler_name AND table_name=$table_name;";
    }

    private static String makePosSelect(String controlTable) {
        return "DECLARE $handler_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "SELECT key_position FROM `" + controlTable + "` "
                + "WHERE handler_name=$handler_name AND table_name=$table_name;";
    }

}
