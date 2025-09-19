package tech.ydb.mv.model;

import java.util.Properties;
import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvDictionarySettings extends MvScanSettings {
    private static final long serialVersionUID = 202500918001L;

    private String historyTableName;
    private String controlTableName;
    private int upsertBatchSize;
    private int threadCount;
    private MvTableInfo historyTableInfo;

    public MvDictionarySettings() {
        this.historyTableName = MvConfig.DEF_DICT_HIST_TABLE;
        this.upsertBatchSize = 500;
        this.threadCount = 4;
    }

    public MvDictionarySettings(MvDictionarySettings other) {
        super(other);
        this.historyTableName = other.historyTableName;
        this.controlTableName = other.controlTableName;
        this.upsertBatchSize = other.upsertBatchSize;
        this.threadCount = other.threadCount;
    }

    public MvDictionarySettings(Properties props) {
        super(props);
        this.historyTableName = props.getProperty(
                MvConfig.CONF_DICT_HIST_TABLE, MvConfig.DEF_DICT_HIST_TABLE);
       String v = props.getProperty(MvConfig.CONF_DEF_BATCH_UPSERT, "500");
        this.upsertBatchSize = Integer.parseInt(v);
        v = props.getProperty(MvConfig.CONF_DEF_CDC_THREADS, "4");
        this.threadCount = Integer.parseInt(v);
    }

    public String getHistoryTableName() {
        return historyTableName;
    }

    public void setHistoryTableName(String historyTableName) {
        this.historyTableName = historyTableName;
    }

    public String getControlTableName() {
        return controlTableName;
    }

    public void setControlTableName(String controlTableName) {
        this.controlTableName = controlTableName;
    }

    public int getUpsertBatchSize() {
        return upsertBatchSize;
    }

    public void setUpsertBatchSize(int upsertBatchSize) {
        this.upsertBatchSize = upsertBatchSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public MvTableInfo getHistoryTableInfo() {
        return historyTableInfo;
    }

    public void setHistoryTableInfo(MvTableInfo historyTableInfo) {
        this.historyTableInfo = historyTableInfo;
    }

}
