package tech.ydb.mv.model;

import java.io.Serializable;
import java.util.Properties;

import tech.ydb.mv.MvConfig;

/**
 * The settings for a specific handler.
 *
 * @author zinal
 */
public class MvHandlerSettings implements Serializable {

    private static final long serialVersionUID = 20251002001L;

    private int cdcReaderThreads = 4;
    private int applyThreads = 4;
    private int applyQueueSize = 10000;
    private int selectBatchSize = 1000;
    private int upsertBatchSize = 500;
    private int dictionaryScanSeconds = 28800; // 8h
    private int queryTimeoutSeconds = 30;

    public MvHandlerSettings() {
    }

    public MvHandlerSettings(MvHandlerSettings src) {
        this.cdcReaderThreads = src.cdcReaderThreads;
        this.applyThreads = src.applyThreads;
        this.applyQueueSize = src.applyQueueSize;
        this.selectBatchSize = src.selectBatchSize;
        this.upsertBatchSize = src.upsertBatchSize;
        this.dictionaryScanSeconds = src.dictionaryScanSeconds;
        this.queryTimeoutSeconds = src.queryTimeoutSeconds;
    }

    public MvHandlerSettings(Properties props) {
        String v;

        v = props.getProperty(MvConfig.CONF_CDC_THREADS, "4");
        this.cdcReaderThreads = Integer.parseInt(v);

        v = props.getProperty(MvConfig.CONF_APPLY_THREADS, "4");
        this.applyThreads = Integer.parseInt(v);

        v = props.getProperty(MvConfig.CONF_APPLY_QUEUE, "10000");
        this.applyQueueSize = Integer.parseInt(v);

        v = props.getProperty(MvConfig.CONF_BATCH_SELECT, "1000");
        this.selectBatchSize = Integer.parseInt(v);

        v = props.getProperty(MvConfig.CONF_BATCH_UPSERT, "500");
        this.upsertBatchSize = Integer.parseInt(v);

        v = props.getProperty(MvConfig.CONF_DICT_SCAN_SECONDS, "28800");
        this.dictionaryScanSeconds = Integer.parseInt(v);

        v = props.getProperty(MvConfig.CONF_QUERY_TIMEOUT, "30");
        this.queryTimeoutSeconds = Integer.parseInt(v);
    }

    public int getCdcReaderThreads() {
        return cdcReaderThreads;
    }

    public void setCdcReaderThreads(int cdcReaderThreads) {
        this.cdcReaderThreads = cdcReaderThreads;
    }

    public int getApplyThreads() {
        return applyThreads;
    }

    public void setApplyThreads(int applyThreads) {
        this.applyThreads = applyThreads;
    }

    public int getApplyQueueSize() {
        return applyQueueSize;
    }

    public void setApplyQueueSize(int applyQueueSize) {
        this.applyQueueSize = applyQueueSize;
    }

    public int getSelectBatchSize() {
        return selectBatchSize;
    }

    public void setSelectBatchSize(int selectBatchSize) {
        this.selectBatchSize = selectBatchSize;
    }

    public int getUpsertBatchSize() {
        return upsertBatchSize;
    }

    public void setUpsertBatchSize(int upsertBatchSize) {
        this.upsertBatchSize = upsertBatchSize;
    }

    public int getDictionaryScanSeconds() {
        return dictionaryScanSeconds;
    }

    public void setDictionaryScanSeconds(int dictionaryScanSeconds) {
        this.dictionaryScanSeconds = dictionaryScanSeconds;
    }

    public int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    public void setQueryTimeoutSeconds(int queryTimeoutSeconds) {
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + this.cdcReaderThreads;
        hash = 37 * hash + this.applyThreads;
        hash = 37 * hash + this.applyQueueSize;
        hash = 37 * hash + this.selectBatchSize;
        hash = 37 * hash + this.upsertBatchSize;
        hash = 37 * hash + this.dictionaryScanSeconds;
        hash = 37 * hash + this.queryTimeoutSeconds;
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
        final MvHandlerSettings other = (MvHandlerSettings) obj;
        if (this.cdcReaderThreads != other.cdcReaderThreads) {
            return false;
        }
        if (this.applyThreads != other.applyThreads) {
            return false;
        }
        if (this.applyQueueSize != other.applyQueueSize) {
            return false;
        }
        if (this.selectBatchSize != other.selectBatchSize) {
            return false;
        }
        if (this.upsertBatchSize != other.upsertBatchSize) {
            return false;
        }
        if (this.queryTimeoutSeconds != other.queryTimeoutSeconds) {
            return false;
        }
        return this.dictionaryScanSeconds == other.dictionaryScanSeconds;
    }

}
