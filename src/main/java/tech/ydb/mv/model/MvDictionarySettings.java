package tech.ydb.mv.model;

import java.io.Serializable;

/**
 *
 * @author zinal
 */
public class MvDictionarySettings implements Serializable {
    private static final long serialVersionUID = 202500908001L;

    private String historyTableName;
    private int upsertBatchSize;

    public MvDictionarySettings() {
        this.historyTableName = "mv/dict_hist";
        this.upsertBatchSize = 500;
    }

    public MvDictionarySettings(MvDictionarySettings other) {
        this.historyTableName = other.historyTableName;
        this.upsertBatchSize = other.upsertBatchSize;
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

}
