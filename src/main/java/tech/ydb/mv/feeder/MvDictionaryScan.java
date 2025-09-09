package tech.ydb.mv.feeder;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;

/**
 * Scans the changes identified for the dictionary, and analyze the effects.
 * The analysis is performed for the particular handler, in order to determine
 * the required update actions over the handler's targets.
 *
 * @author zinal
 */
public class MvDictionaryScan {

    private final MvHandler handler;
    private final YdbConnector conn;
    private final MvDictionarySettings settings;
    private final String historyTable;

    public MvDictionaryScan(MvHandler handler, YdbConnector conn,
            MvDictionarySettings settings) {
        this.handler = handler;
        this.conn = conn;
        this.settings = new MvDictionarySettings(settings);
        this.historyTable = YdbConnector.safe(settings.getHistoryTableName());
    }

}
