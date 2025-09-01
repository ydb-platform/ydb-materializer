package tech.ydb.mv.feeder;

import tech.ydb.query.tools.SessionRetryContext;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class MvScanContext {

    private final MvHandler handler;
    private final MvTarget target;
    private final SessionRetryContext retryCtx;

    private final String sqlPosUpsert;
    private final String sqlPosDelete;
    private final String sqlPosSelect;
    private final String sqlSelectStart;
    private final String sqlSelectNext;

    private final Value<?> handlerName;
    private final Value<?> targetName;

    public MvScanContext(MvHandler handler, MvTarget target, YdbConnector ydb) {
        this.handler = handler;
        this.target = target;
        this.retryCtx = ydb.getQueryRetryCtx();
        String controlTable = ydb.getProperty(MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        this.sqlPosUpsert = makePosUpsert(controlTable);
        this.sqlPosDelete = makePosDelete(controlTable);
        this.sqlPosSelect = makePosSelect(controlTable);
        this.sqlSelectStart = makeSelectStart(target);
        this.sqlSelectNext = makeSelectNext(target);
        this.handlerName = PrimitiveValue.newText(handler.getName());
        this.targetName = PrimitiveValue.newText(target.getName());
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

    private static void keyNamesByComma(StringBuilder sb, MvTableInfo topmost) {
        int index = 0;
        for (String name : topmost.getKey()) {
            if (index++ > 0) {
                sb.append(", ");
            }
            sb.append("`").append(name).append("`");
        }
    }

    public static String makeSelectNext(MvTarget target) {
        MvTableInfo topmost = target.getTopMostSource().getTableInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $limit AS Uint64;").append(MvSqlGen.EOL);
        int index = 0;
        for (String name : topmost.getKey()) {
            sb.append("DECLARE $c").append(++index).append(" AS ");
            sb.append(topmost.getColumns().get(name));
            sb.append(";").append(MvSqlGen.EOL);
        }
        sb.append("SELECT ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("FROM `").append(topmost.getName()).append("`");
        sb.append(MvSqlGen.EOL);
        sb.append("WHERE (");
        keyNamesByComma(sb, topmost);
        sb.append(") > (");
        index = 0;
        for (String name : topmost.getKey()) {
            if (index++ > 0) {
                sb.append(", ");
            }
            sb.append("$c").append(index);
        }
        sb.append(")").append(MvSqlGen.EOL);
        sb.append("ORDER BY ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("LIMIT $limit;");
        sb.append(MvSqlGen.EOL);
        return sb.toString();
    }

    public static String makeSelectStart(MvTarget target) {
        MvTableInfo topmost = target.getTopMostSource().getTableInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $limit AS Uint64;").append(MvSqlGen.EOL);
        sb.append("SELECT ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("FROM `").append(topmost.getName()).append("`");
        sb.append(MvSqlGen.EOL);
        sb.append("ORDER BY ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("LIMIT $limit;");
        sb.append(MvSqlGen.EOL);
        return sb.toString();
    }

}
