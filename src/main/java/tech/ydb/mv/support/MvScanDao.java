package tech.ydb.mv.support;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.YdbStruct;

/**
 *
 * @author zinal
 */
public class MvScanDao {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvScanDao.class);

    private final YdbConnector conn;
    private final MvScanAdapter adapter;
    private final String sqlPosSelect;
    private final String sqlPosUpsert;
    private final String sqlPosDelete;

    public MvScanDao(YdbConnector conn, MvScanAdapter adapter) {
        this.conn = conn;
        this.adapter = adapter;
        this.sqlPosSelect = makePosSelect(adapter);
        this.sqlPosUpsert = makePosUpsert(adapter);
        this.sqlPosDelete = makePosDelete(adapter);
    }

    public MvKey initScan() {
        LOG.debug("Initiating scan, handler `{}`, table `{}`",
                adapter.getJobName(), adapter.getTableName());
        Params params = Params.of(
                "$job_name", PrimitiveValue.newText(adapter.getJobName()),
                "$table_name", PrimitiveValue.newText(adapter.getTableName())
        );
        ResultSetReader rsr = conn.sqlRead(sqlPosSelect, params).getResultSet(0);
        MvKey key = null;
        if (rsr.next()) {
            YdbStruct ys = YdbStruct.fromJson(rsr.getColumn(0).getJsonDocument());
            key = new MvKey(ys, adapter.getTableInfo());
        }
        return key;
    }

    public void registerScan() {
        LOG.debug("Registering scan, handler `{}`, table `{}`",
                adapter.getJobName(), adapter.getTableName());
        Params params = Params.of(
                "$job_name", PrimitiveValue.newText(adapter.getJobName()),
                "$table_name", PrimitiveValue.newText(adapter.getTableName()),
                "$key_position", PrimitiveValue.newJsonDocument("{}")
        );
        conn.sqlWrite(sqlPosUpsert, params);
    }

    public void unregisterScan() {
        LOG.debug("Unregistering scan, handler `{}`, table `{}`",
                adapter.getJobName(), adapter.getTableName());
        Params params = Params.of(
                "$job_name", PrimitiveValue.newText(adapter.getJobName()),
                "$table_name", PrimitiveValue.newText(adapter.getTableName())
        );
        conn.sqlWrite(sqlPosDelete, params);
    }

    public void saveScan(MvKey key) {
        LOG.debug("Saving scan position, handler `{}`, table `{}`",
                adapter.getJobName(), adapter.getTableName());
        Params params = Params.of(
                "$job_name", PrimitiveValue.newText(adapter.getJobName()),
                "$table_name", PrimitiveValue.newText(adapter.getTableName()),
                "$key_position", PrimitiveValue.newJsonDocument(key.convertKeyToJson())
        );
        conn.sqlWrite(sqlPosUpsert, params);
    }

    public void unregisterSpecificScan(String tableName) {
        LOG.debug("Unregistering scan, handler `{}`, table `{}`",
                adapter.getJobName(), tableName);
        Params params = Params.of(
                "$job_name", PrimitiveValue.newText(adapter.getJobName()),
                "$table_name", PrimitiveValue.newText(tableName)
        );
        conn.sqlWrite(sqlPosDelete, params);
    }

    private static String makePosUpsert(MvScanAdapter adapter) {
        return "DECLARE $job_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "DECLARE $key_position AS JsonDocument; "
                + "UPSERT INTO `" + MvConfig.safe(adapter.getControlTable()) + "` "
                + "(job_name, table_name, updated_at, key_position) "
                + "VALUES ($job_name, $table_name, CurrentUtcTimestamp(), $key_position);";
    }

    private static String makePosDelete(MvScanAdapter adapter) {
        return "DECLARE $job_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "DELETE FROM `" + MvConfig.safe(adapter.getControlTable()) + "` "
                + "WHERE job_name=$job_name AND table_name=$table_name;";
    }

    private static String makePosSelect(MvScanAdapter adapter) {
        return "DECLARE $job_name AS Text; "
                + "DECLARE $table_name AS Text; "
                + "SELECT key_position FROM `" + MvConfig.safe(adapter.getControlTable()) + "` "
                + "WHERE job_name=$job_name AND table_name=$table_name;";
    }

}
