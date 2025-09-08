package tech.ydb.mv.model;

import java.io.Serializable;
import java.util.Properties;
import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvDictionarySettings implements Serializable {
    private static final long serialVersionUID = 202500908001L;

    private String historyTableName;
    private int upsertBatchSize;
    private int threadCount;

    public MvDictionarySettings() {
        this.historyTableName = "mv/dict_hist";
        this.upsertBatchSize = 500;
        this.threadCount = 4;
    }

    public MvDictionarySettings(MvDictionarySettings other) {
        this.historyTableName = other.historyTableName;
        this.upsertBatchSize = other.upsertBatchSize;
        this.threadCount = other.threadCount;
    }

    public MvDictionarySettings(Properties props) {
        this.historyTableName = props.getProperty(
                MvConfig.CONF_DICT_TABLE, MvConfig.DEF_DICT_TABLE);
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

}
