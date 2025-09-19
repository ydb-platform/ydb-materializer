package tech.ydb.mv.dict;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.MvRowFilter;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.parser.MvMetadataReader;
import tech.ydb.mv.support.MvScanAdapter;
import tech.ydb.mv.support.MvScanDao;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

/**
 * Scans the changes identified for the dictionary, and analyze the effects. The
 * analysis is performed for the particular handler, in order to determine the
 * required update actions over the handler's targets.
 *
 * @author zinal
 */
public class MvDictionaryScan implements MvScanAdapter {

    private final YdbConnector conn;
    private final MvHandler handler;
    private final MvDictionarySettings settings;
    private final String sourceTableName;
    private final MvTableInfo historyTableInfo;

    public MvDictionaryScan(YdbConnector conn, MvHandler handler,
            MvDictionarySettings settings, String sourceTableName) {
        this.handler = handler;
        this.conn = conn;
        this.settings = new MvDictionarySettings(settings);
        this.sourceTableName = sourceTableName;
        this.historyTableInfo = grabHistoryTableInfo(conn, settings);
    }

    private MvTableInfo grabHistoryTableInfo(YdbConnector conn, MvDictionarySettings settings) {
        if (settings.getHistoryTableInfo() != null) {
            return settings.getHistoryTableInfo();
        }
        return new MvMetadataReader(conn).describeTable(settings.getHistoryTableName());
    }

    @Override
    public String getControlTable() {
        // Normally there is a single control table per whole database.
        return settings.getControlTableName();
    }

    @Override
    public String getHandlerName() {
        // Each handler has its own dictionary log scan context.
        return handler.getName();
    }

    @Override
    public String getTargetName() {
        // Here we report the source table name as the target, unlike regular scans.
        return sourceTableName;
   }

    @Override
    public MvTableInfo getTableInfo() {
        // The actual table being scanned is the dictionary history table.
        return historyTableInfo;
    }

    public void run() {
        MvScanDao scanDao = new MvScanDao(conn, this);
        MvKey key = scanDao.initScan();
        ScanResult result = scan(key);
        if (result.lastKey != null && !result.lastKey.isEmpty()) {
            scanDao.saveScan(result.lastKey);
        }
    }

    private ScanResult scan(MvKey startKey) {
        Params params;
        String sql;
        if (startKey == null || startKey.isEmpty()) {
            params = Params.of("$src", PrimitiveValue.newText(sourceTableName));
            sql = "DECLARE $src AS Text; "
                    + "SELECT src, tv, key_text, key_val, diff_val FROM `"
                    + YdbConnector.safe(settings.getHistoryTableName())
                    + "` WHERE src=$src;"
                    + "ORDER BY src, tv, key_text;";
        } else {
            params = Params.of(
                    "$src", PrimitiveValue.newText(sourceTableName),
                    "$tv", startKey.convertValue(1),
                    "$key_text", startKey.convertValue(2));
            sql = "DECLARE $src AS Text; DECLARE $tv AS Timestamp; "
                    + "DECLARE $key_text AS Text; "
                    + "SELECT src, tv, key_text, key_val, diff_val FROM `"
                    + YdbConnector.safe(settings.getHistoryTableName())
                    + "` WHERE src=$src AND (tv, key_text) > ($tv, $key_text)"
                    + "ORDER BY src, tv, key_text ";
        }
        ResultSetReader rsr = conn.sqlRead(sql, params).getResultSet(0);
        ScanResult result = new ScanResult();
        while (rsr.next()) {

        }
        return result;
    }

    static class ScanResult {

        MvKey lastKey = null;
        final MvRowFilter filter = new MvRowFilter();
    }
}
