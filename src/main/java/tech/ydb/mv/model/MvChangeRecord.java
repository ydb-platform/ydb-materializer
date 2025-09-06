package tech.ydb.mv.model;

import tech.ydb.mv.util.YdbStruct;

/**
 *
 * @author zinal
 */
public class MvChangeRecord {

    private final MvKey key;
    private final OpType operationType;
    private final YdbStruct imageBefore;
    private final YdbStruct imageAfter;

    public MvChangeRecord(MvKey key) {
        this(key, OpType.UPSERT);
    }

    public MvChangeRecord(MvKey key, OpType operationType) {
        this.key = key;
        this.operationType = operationType;
        this.imageBefore = YdbStruct.EMPTY;
        this.imageAfter = YdbStruct.EMPTY;
    }

    public MvChangeRecord(MvKey key, OpType operationType,
            YdbStruct imageBefore, YdbStruct imageAfter) {
        this.key = key;
        this.operationType = operationType;
        this.imageBefore = (imageBefore == null) ? YdbStruct.EMPTY : imageBefore;
        this.imageAfter = (imageAfter == null) ? YdbStruct.EMPTY : imageAfter;
    }

    public MvKey getKey() {
        return key;
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
        return "MvChangeRecord{" + "key=" + key + ", op=" + operationType
                + ", before=" + imageBefore + ", after=" + imageAfter + '}';
    }

    public static enum OpType {
        UPSERT,
        DELETE
    }

}
