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

    private static final long serialVersionUID = 20250921001L;

    private int cdcReaderThreads = 4;
    private int applyThreads = 4;
    private int applyQueueSize = 10000;
    private int selectBatchSize = 1000;
    private int upsertBatchSize = 500;
    private int dictionaryScanSeconds = 28800; // 8h

    public MvHandlerSettings() {
    }

    public MvHandlerSettings(MvHandlerSettings src) {
        this.cdcReaderThreads = src.cdcReaderThreads;
        this.applyThreads = src.applyThreads;
        this.applyQueueSize = src.applyQueueSize;
        this.selectBatchSize = src.selectBatchSize;
        this.upsertBatchSize = src.upsertBatchSize;
        this.dictionaryScanSeconds = src.dictionaryScanSeconds;
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

}
