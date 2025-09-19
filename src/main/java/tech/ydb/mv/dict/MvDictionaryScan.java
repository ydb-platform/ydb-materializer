package tech.ydb.mv.dict;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.parser.MvMetadataReader;
import tech.ydb.mv.support.MvScanAdapter;

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

    }

}
