package tech.ydb.mv.model;

/**
 *
 * @author zinal
 */
public class MvWorkSettings {

    private int selectBatchSize = 1000;
    private int upsertBatchSize = 500;

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
