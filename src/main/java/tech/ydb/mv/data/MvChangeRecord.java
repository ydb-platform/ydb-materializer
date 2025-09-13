package tech.ydb.mv.data;

import java.time.Instant;


/**
 *
 * @author zinal
 */
public class MvChangeRecord {

    private final MvKey key;
    private final Instant tv;
    private final OpType operationType;
    private final YdbStruct imageBefore;
    private final YdbStruct imageAfter;

    public MvChangeRecord(MvKey key, Instant tv) {
        this(key, tv, OpType.UPSERT);
    }

    public MvChangeRecord(MvKey key, Instant tv, OpType operationType) {
        this.key = key;
        this.tv = tv;
        this.operationType = operationType;
        this.imageBefore = YdbStruct.EMPTY;
        this.imageAfter = YdbStruct.EMPTY;
    }

    public MvChangeRecord(MvKey key, Instant tv, OpType operationType,
            YdbStruct imageBefore, YdbStruct imageAfter) {
        this.key = key;
        this.tv = tv;
        this.operationType = operationType;
        this.imageBefore = (imageBefore == null) ? YdbStruct.EMPTY : imageBefore;
        this.imageAfter = (imageAfter == null) ? YdbStruct.EMPTY : imageAfter;
    }

    public MvKey getKey() {
        return key;
    }

    public Instant getTv() {
        return tv;
    }

    public OpType getOperationType() {
        return operationType;
    }

    public YdbStruct getImageBefore() {
        return imageBefore;
    }

    public YdbStruct getImageAfter() {
        return imageAfter;
    }

    @Override
    public String toString() {
        return "CR{" + "key=" + key + ", op=" + operationType
                + ", before=" + imageBefore + ", after=" + imageAfter + '}';
    }

    public static enum OpType {
        UPSERT,
        DELETE
    }

}
