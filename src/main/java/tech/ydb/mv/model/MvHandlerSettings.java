package tech.ydb.mv.model;

import java.io.Serializable;

/**
 * The settings for a specific handler.
 *
 * @author zinal
 */
public class MvHandlerSettings implements Serializable {
    private static final long serialVersionUID = 20250822001L;

    private int cdcReaderThreads = 4;
    private int applyThreads = 4;
    private int applyQueueSize = 10000;
    private int selectBatchSize = 1000;
    private int upsertBatchSize = 500;

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

}
