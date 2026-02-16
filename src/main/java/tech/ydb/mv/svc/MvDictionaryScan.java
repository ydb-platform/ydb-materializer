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
        this.controlTableName = conn.getProperty(MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        this.historyTableName = conn.getProperty(MvConfig.CONF_DICT_HIST_TABLE, MvConfig.DEF_DICT_HIST_TABLE);
        this.historyTableInfo = describer.describeTable(this.historyTableName, null);
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
        var result = new MvChangesSingleDict(tableName);
        var scanner = new Scanner(tableName, result);
        MvKey startKey = new MvScanDao(conn, scanner).initScan();
        MvKey curKey = startKey;
        result.setScanPosition(startKey);

        LOG.debug("[{}] Scanning dictionary `{}` at position {}",
                handler.getName(), tableName, startKey);

        long changeRowsScanned = 0;
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
                curKey = scanner.handleRow(curKey, rsr);
                ++changeRowsScanned;
            }
            if (changeRowsScanned > scanLimit) {
                LOG.warn("[{}] Dictionary changes scan for table `{}` stopped "
                        + "before reaching EOF because it got {} rows, limit is {} rows.",
                        handler.getName(), tableName, changeRowsScanned, scanLimit);
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
        LOG.debug("[{}] Performing regular dictionary changes scan.", handler.getName());
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
            LOG.info("[{}] Relevant changes in the following dictionaries: {}",
                    handler.getName(), items);
        } else {
            LOG.debug("[{}] No relevant changes in the dictionaries.",
                    handler.getName());
        }
        LOG.debug("[{}] Regular dictionary changes scan completed.", handler.getName());
        return ret;
    }

    public void commitAll(MvChangesMultiDict cmd) {
        cmd.getItems().forEach(item -> commit(item));
    }

    class Adapter implements MvScanAdapter {

        final String sourceTableName;
        final MvTableInfo sourceTableInfo;

        Adapter(String sourceTableName) {
            this.sourceTableName = sourceTableName;
            this.sourceTableInfo = describer.describeTable(sourceTableName, null);
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

    class Scanner extends Adapter {

        private final MvChangesSingleDict result;

        public Scanner(String sourceTableName, MvChangesSingleDict result) {
            super(sourceTableName);
            this.result = result;
        }

        MvKey handleRow(MvKey curKey, ResultSetReader rsr) {
            curKey = new MvKey(rsr, historyTableInfo.getKeyInfo());
            result.setScanPosition(curKey);
            String diffStr = rsr.getColumn(5).getJsonDocument();
            if (diffStr == null) {
                if (!result.isMissingDiffFieldRows()) {
                    LOG.warn("[{}] Missing value in the `diff_val` field with key {} "
                            + "of the `{}` table, row skipped. Further messages suppressed.",
                            handler.getName(), curKey, historyTableName);
                    result.setMissingDiffFieldRows(true);
                }
                return curKey;
            }
            JsonElement diffObj = JsonParser.parseString(diffStr);
            if (!diffObj.isJsonObject()) {
                if (!result.isMissingDiffFieldRows()) {
                    LOG.warn("[{}] Illegal format value in the `diff_val` field with key {} "
                            + "of the `{}` table, row skipped. Further messages suppressed.",
                            handler.getName(), curKey, historyTableName);
                    result.setMissingDiffFieldRows(true);
                }
                return curKey;
            }
            MvKey rowKey = new MvKey(rsr.getColumn(4).getJsonDocument(),
                    sourceTableInfo.getKeyInfo());
            JsonArray diffArray = diffObj.getAsJsonObject().getAsJsonArray("f");
            for (JsonElement item : diffArray.asList()) {
                result.updateField(item.getAsString(), rowKey);
            }
            return curKey;
        }
    }

}
