package tech.ydb.mv.model;

import java.util.Objects;
import java.util.Properties;
import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvDictionarySettings extends MvScanSettings {

    private static final long serialVersionUID = 202500926001L;

    private String historyTableName;
    private int upsertBatchSize;
    private int threadCount;

    public MvDictionarySettings() {
        this.historyTableName = MvConfig.DEF_DICT_HIST_TABLE;
        this.upsertBatchSize = 500;
        this.threadCount = 4;
    }

    public MvDictionarySettings(MvDictionarySettings other) {
        super(other);
        this.historyTableName = other.historyTableName;
        this.upsertBatchSize = other.upsertBatchSize;
        this.threadCount = other.threadCount;
    }

    public MvDictionarySettings(Properties props) {
        super(props);
        this.historyTableName = props.getProperty(
                MvConfig.CONF_DICT_HIST_TABLE, MvConfig.DEF_DICT_HIST_TABLE);
        String v = props.getProperty(MvConfig.CONF_BATCH_UPSERT, "500");
        this.upsertBatchSize = Integer.parseInt(v);
        v = props.getProperty(MvConfig.CONF_CDC_THREADS, "4");
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

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.historyTableName);
        hash = 89 * hash + this.upsertBatchSize;
        hash = 89 * hash + this.threadCount;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvDictionarySettings other = (MvDictionarySettings) obj;
        if (this.upsertBatchSize != other.upsertBatchSize) {
            return false;
        }
        if (this.threadCount != other.threadCount) {
            return false;
        }
        return Objects.equals(this.historyTableName, other.historyTableName);
    }

}
