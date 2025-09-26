package tech.ydb.mv.model;

import java.util.Properties;
import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvDictionarySettings extends MvScanSettings {

    private static final long serialVersionUID = 202500926001L;

    private int upsertBatchSize;
    private int cdcReaderThreads;

    public MvDictionarySettings() {
        this.upsertBatchSize = 500;
        this.cdcReaderThreads = 4;
    }

    public MvDictionarySettings(MvDictionarySettings other) {
        super(other);
        this.upsertBatchSize = other.upsertBatchSize;
        this.cdcReaderThreads = other.cdcReaderThreads;
    }

    public MvDictionarySettings(Properties props) {
        super(props);
        String v = props.getProperty(MvConfig.CONF_BATCH_UPSERT, "500");
        this.upsertBatchSize = Integer.parseInt(v);
        v = props.getProperty(MvConfig.CONF_CDC_THREADS, "4");
        this.cdcReaderThreads = Integer.parseInt(v);
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

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + this.upsertBatchSize;
        hash = 89 * hash + this.cdcReaderThreads;
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
        return super.equals(obj);
    }

}
