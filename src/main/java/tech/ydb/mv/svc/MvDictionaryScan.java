package tech.ydb.mv.svc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.data.MvChangesMultiDict;
import tech.ydb.mv.data.MvChangesSingleDict;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.parser.MvDescriber;
import tech.ydb.mv.support.MvScanAdapter;
import tech.ydb.mv.support.MvScanDao;

/**
 * Scans the changes identified for the dictionary, and analyze the effects. The
 * analysis is performed for the particular handler, in order to determine the
 * required update actions over the handler's targets.
 *
 * @author zinal
 */
public class MvDictionaryScan {

    private final YdbConnector conn;
    private final MvHandler handler;
    private final MvDictionarySettings settings;
    private final MvDescriber describer;
    private final MvTableInfo historyTableInfo;

    public MvDictionaryScan(YdbConnector conn, MvDescriber describer,
            MvHandler handler, MvDictionarySettings settings) {
        this.handler = handler;
        this.conn = conn;
        this.settings = new MvDictionarySettings(settings);
        this.describer = describer;
        this.historyTableInfo = describer.describeTable(settings.getHistoryTableName());
    }

    public MvChangesSingleDict scan(String tableName) {
        var adapter = new Adapter(tableName);
        MvKey startKey = new MvScanDao(conn, adapter).initScan();

        Params params;
        String sql;
        if (startKey == null || startKey.isEmpty()) {
            params = Params.of("$src", PrimitiveValue.newText(tableName));
            sql = "DECLARE $src AS Text; "
                    + "SELECT src, tv, seqno, key_text, key_val, diff_val FROM `"
                    + YdbConnector.safe(settings.getHistoryTableName())
                    + "` WHERE src=$src;"
                    + "ORDER BY src, tv, seqno, key_text;";
        } else {
            params = Params.of(
                    "$src", PrimitiveValue.newText(tableName),
                    "$tv", startKey.convertValue(1),
                    "$seqno", startKey.convertValue(2),
                    "$key_text", startKey.convertValue(3)
            );
            sql = "DECLARE $src AS Text; DECLARE $tv AS Timestamp; "
                    + "DECLARE $seqno AS Uint64; DECLARE $key_text AS Text; "
                    + "SELECT src, tv, seqno, key_text, key_val, diff_val FROM `"
                    + YdbConnector.safe(settings.getHistoryTableName())
                    + "` WHERE src=$src AND (tv, seqno, key_text) > ($tv, $seqno, $key_text) "
                    + "ORDER BY src, tv, seqno, key_text;";
        }
        ResultSetReader rsr = conn.sqlRead(sql, params).getResultSet(0);
        var result = new MvChangesSingleDict(tableName);
        result.setScanPosition(startKey);
        while (rsr.next()) {
            result.setScanPosition(new MvKey(rsr, historyTableInfo.getKeyInfo()));
            String diffStr = rsr.getColumn(5).getText();
            if (diffStr==null) {
                continue;
            }
            JsonElement diffObj = JsonParser.parseString(diffStr);
            if (! diffObj.isJsonObject()) {
                continue;
            }
            MvKey rowKey = new MvKey(rsr.getColumn(4).getJsonDocument(),
                    adapter.sourceTableInfo.getKeyInfo());
            JsonArray diffArray = diffObj.getAsJsonObject().getAsJsonArray("f");
            for (JsonElement item : diffArray.asList()) {
                result.updateField(item.getAsString(), rowKey);
            }
        }
        return result;
    }

    public void commit(MvChangesSingleDict mdc) {
        var scanDao = new MvScanDao(conn, new Adapter(mdc.getTableName()));
        if (mdc.getScanPosition() == null || mdc.getScanPosition().isEmpty()) {
            scanDao.unregisterScan();
        } else {
            scanDao.saveScan(mdc.getScanPosition());
        }
    }

    public MvChangesMultiDict scanAll() {
        MvChangesMultiDict ret = new MvChangesMultiDict();
        handler.getInputs().values().stream()
                .filter(i -> i.isBatchMode())
                .filter(i -> i.isTableKnown())
                .map(i -> i.getTableName())
                .forEach(tableName -> ret.addItem(scan(tableName)));
        return ret;
    }

    public void commitAll(MvChangesMultiDict cmd) {
        cmd.getItems().forEach(item -> commit(item));
    }

    class Adapter implements MvScanAdapter {

        private final String sourceTableName;
        private final MvTableInfo sourceTableInfo;

        public Adapter(String sourceTableName) {
            this.sourceTableName = sourceTableName;
            this.sourceTableInfo = describer.describeTable(sourceTableName);
        }

        @Override
        public MvTableInfo getTableInfo() {
            // The actual table being scanned is the dictionary history table.
            return historyTableInfo;
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

    }

}
