package tech.ydb.mv.model;

import java.util.Properties;

import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvDictionarySettings extends MvScanSettings {

    private static final long serialVersionUID = 202501228001L;

    private int upsertBatchSize;
    private int cdcReaderThreads;
    private int maxChangeRowsScanned;

    public MvDictionarySettings() {
        this.upsertBatchSize = 500;
        this.cdcReaderThreads = 4;
        this.maxChangeRowsScanned = 100000;
    }

    public MvDictionarySettings(MvDictionarySettings other) {
        super(other);
        this.upsertBatchSize = other.upsertBatchSize;
        this.cdcReaderThreads = other.cdcReaderThreads;
        this.maxChangeRowsScanned = other.maxChangeRowsScanned;
    }

    public MvDictionarySettings(Properties props) {
        super(props);
        this.upsertBatchSize = MvConfig.parseInt(props, MvConfig.CONF_BATCH_UPSERT, 500);
        this.cdcReaderThreads = MvConfig.parseInt(props, MvConfig.CONF_CDC_THREADS, 4);
        this.maxChangeRowsScanned = MvConfig.parseInt(props, MvConfig.CONF_MAX_ROW_CHANGES, 100000);
    }

    public int getUpsertBatchSize() {
        return upsertBatchSize;
    }

    public void setUpsertBatchSize(int upsertBatchSize) {
        this.upsertBatchSize = upsertBatchSize;
    }

    public int getCdcReaderThreads() {
        return cdcReaderThreads;
    }

    public void setCdcReaderThreads(int threadCount) {
        this.cdcReaderThreads = threadCount;
    }

    public int getMaxChangeRowsScanned() {
        return maxChangeRowsScanned;
    }

    public void setMaxChangeRowsScanned(int maxChangeRowsScanned) {
        this.maxChangeRowsScanned = maxChangeRowsScanned;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + this.upsertBatchSize;
        hash = 89 * hash + this.cdcReaderThreads;
        hash = 89 * hash + this.maxChangeRowsScanned;
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
        if (this.cdcReaderThreads != other.cdcReaderThreads) {
            return false;
        }
        if (this.maxChangeRowsScanned != other.maxChangeRowsScanned) {
            return false;
        }
        return super.equals(obj);
    }

}
