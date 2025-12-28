package tech.ydb.mv.svc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.MvConfig;
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

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvDictionaryScan.class);

    private final YdbConnector conn;
    private final MvHandler handler;
    private final MvDictionarySettings settings;
    private final MvDescriber describer;
    private final String controlTableName;
    private final String historyTableName;
    private final MvTableInfo historyTableInfo;
    private final String sqlSelectInitial;
    private final String sqlSelectNext;

    public MvDictionaryScan(YdbConnector conn, MvDescriber describer,
            MvHandler handler, MvDictionarySettings settings) {
        this.handler = handler;
        this.conn = conn;
        this.settings = new MvDictionarySettings(settings);
        this.describer = describer;
        this.controlTableName = conn.getProperty(
                MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        this.historyTableName = conn.getProperty(
                MvConfig.CONF_DICT_HIST_TABLE, MvConfig.DEF_DICT_HIST_TABLE);
        this.historyTableInfo = describer.describeTable(this.historyTableName);
        this.sqlSelectInitial = getSelectInitial(this.historyTableName);
        this.sqlSelectNext = getSelectNext(this.historyTableName);
    }

    public MvHandler getHandler() {
        return handler;
    }

    private static String getSelectInitial(String historyTableName) {
        return "DECLARE $src AS Text; "
                + "SELECT src, tv, seqno, key_text, key_val, diff_val FROM `"
                + historyTableName
                + "` WHERE src=$src "
                + "ORDER BY src, tv, seqno, key_text "
                + "LIMIT 500;";
    }

    private static String getSelectNext(String historyTableName) {
        return "DECLARE $src AS Text; DECLARE $tv AS Timestamp; "
                + "DECLARE $seqno AS Uint64; DECLARE $key_text AS Text; "
                + "SELECT src, tv, seqno, key_text, key_val, diff_val FROM `"
                + historyTableName
                + "` WHERE src=$src AND (tv, seqno, key_text) > ($tv, $seqno, $key_text) "
                + "ORDER BY src, tv, seqno, key_text "
                + "LIMIT 500;";
    }

    public MvChangesSingleDict scan(String tableName) {
        long scanLimit = settings.getMaxChangeRowsScanned();
        if (scanLimit <= 0) {
            scanLimit = Long.MAX_VALUE;
        }
        var adapter = new Adapter(tableName);
        MvKey startKey = new MvScanDao(conn, adapter).initScan();
        MvKey curKey = startKey;

        LOG.info("\t...dictionary `{}` at position {}", tableName, startKey);

        var result = new MvChangesSingleDict(tableName);
        result.setScanPosition(startKey);

        long changeRowsScanned = 0;
        boolean hasMissingDiffFieldRows = false;
        var pTableName = PrimitiveValue.newText(tableName);
        ResultSetReader rsr;
        do {
            Params params;
            String sql;
            if (curKey == null || curKey.isEmpty()) {
                params = Params.of("$src", pTableName);
                sql = sqlSelectInitial;
            } else {
                params = Params.of(
                        "$src", pTableName,
                        "$tv", curKey.convertValue(1),
                        "$seqno", curKey.convertValue(2),
                        "$key_text", curKey.convertValue(3)
                );
                sql = sqlSelectNext;
            }
            rsr = conn.sqlRead(sql, params).getResultSet(0);
            while (rsr.next()) {
                curKey = new MvKey(rsr, historyTableInfo.getKeyInfo());
                result.setScanPosition(curKey);
                String diffStr = rsr.getColumn(5).getJsonDocument();
                if (diffStr == null) {
                    if (!hasMissingDiffFieldRows) {
                        LOG.warn("Missing value in the `diff_val` field with key {} "
                                + "of the `{}` table, row skipped. Further messages suppressed.",
                                curKey, historyTableName);
                        hasMissingDiffFieldRows = true;
                    }
                    continue;
                }
                JsonElement diffObj = JsonParser.parseString(diffStr);
                if (!diffObj.isJsonObject()) {
                    if (!hasMissingDiffFieldRows) {
                        LOG.warn("Illegal format value in the `diff_val` field with key {} "
                                + "of the `{}` table, row skipped. Further messages suppressed.",
                                curKey, historyTableName);
                        hasMissingDiffFieldRows = true;
                    }
                    continue;
                }
                MvKey rowKey = new MvKey(rsr.getColumn(4).getJsonDocument(),
                        adapter.sourceTableInfo.getKeyInfo());
                JsonArray diffArray = diffObj.getAsJsonObject().getAsJsonArray("f");
                for (JsonElement item : diffArray.asList()) {
                    result.updateField(item.getAsString(), rowKey);
                }
                ++changeRowsScanned;
            }
            if (changeRowsScanned > scanLimit) {
                LOG.warn("Dictionary changes scan for table `{}` stopped "
                        + "before reaching EOF because it got {} rows, limit is {} rows.",
                        tableName, changeRowsScanned, scanLimit);
                break;
            }
        } while (rsr.getRowCount() > 0);

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
        LOG.info("Performing regular dictionary changes scan.");
        MvChangesMultiDict ret = new MvChangesMultiDict();
        handler.getInputs().values().stream()
                .filter(i -> i.isBatchMode())
                .filter(i -> i.isTableKnown())
                .map(i -> i.getTableName())
                .forEach(tableName -> ret.addItem(scan(tableName)));
        if (!ret.isEmpty()) {
            var items = ret.getItems().stream()
                    .filter(i -> !i.isEmpty())
                    .map(i -> i.getTableName())
                    .toList();
            LOG.info("Relevant changes in the following dictionaries: {}", items);
        }
        LOG.info("Regular dictionary changes scan completed.");
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
            return controlTableName;
        }

        @Override
        public String getJobName() {
            // Each handler has its own dictionary log scan context.
            return handler.getName();
        }

        @Override
        public String getTableName() {
            // Here we report the source table name as the target, unlike regular scans.
            return sourceTableName;
        }

    }

}
