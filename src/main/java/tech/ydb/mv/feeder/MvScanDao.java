package tech.ydb.mv.feeder;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.YdbStruct;

/**
 *
 * @author zinal
 */
class MvScanDao {

    private final YdbConnector conn;
    private final MvScanAdapter adapter;
    private final String sqlPosSelect;
    private final String sqlPosUpsert;
    private final String sqlPosDelete;

    public MvScanDao(YdbConnector conn, MvScanAdapter adapter) {
        this.conn = conn;
        this.adapter = adapter;
        this.sqlPosSelect = makePosSelect(adapter.getControlTable());
        this.sqlPosUpsert = makePosUpsert(adapter.getControlTable());
        this.sqlPosDelete = makePosDelete(adapter.getControlTable());
    }

    public MvKey initScan() {
        Params params = Params.of(
                "$handler_name", PrimitiveValue.newText(adapter.getHandlerName()),
                "$table_name", PrimitiveValue.newText(adapter.getTargetName())
        );
        ResultSetReader rsr = conn.sqlRead(sqlPosSelect, params).getResultSet(0);
        MvKey key = null;
        if (rsr.next()) {
            YdbStruct ys = YdbStruct.fromJson(rsr.getColumn(0).getText());
            key = new MvKey(ys, adapter.getTableInfo());
        }
        return key;
    }

    public void registerScan() {
        Params params = Params.of(
                "$handler_name", PrimitiveValue.newText(adapter.getHandlerName()),
                "$table_name", PrimitiveValue.newText(adapter.getTargetName()),
                "$key_position", PrimitiveValue.newJsonDocument("{}")
        );
        conn.sqlWrite(sqlPosUpsert, params);
    }

    public void unregisterScan() {
        Params params = Params.of(
                "$handler_name", PrimitiveValue.newText(adapter.getHandlerName()),
                "$table_name", PrimitiveValue.newText(adapter.getTargetName())
        );
        conn.sqlWrite(sqlPosDelete, params);
    }

    public void saveScan(MvKey key) {
        Params params = Params.of(
                "$handler_name", PrimitiveValue.newText(adapter.getHandlerName()),
                "$table_name", PrimitiveValue.newText(adapter.getTargetName()),
                "$key_position", PrimitiveValue.newJsonDocument(key.convertKeyToJson())
        );
        conn.sqlWrite(sqlPosUpsert, params);
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
