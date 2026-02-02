package tech.ydb.mv.data;

import java.time.Instant;

/**
 * Change record produced by CDC/scan for a single table key.
 *
 * Carries operation type and optional "before"/"after" images.
 *
 * @author zinal
 */
public class MvChangeRecord {

    private final MvKey key;
    private final Instant tv;
    private final OpType operationType;
    private final YdbStruct imageBefore;
    private final YdbStruct imageAfter;

    /**
     * Create an UPSERT change record without images.
     *
     * @param key Table key.
     * @param tv Timestamp/version of the change.
     */
    public MvChangeRecord(MvKey key, Instant tv) {
        this(key, tv, OpType.UPSERT);
    }

    /**
     * Create a change record without images.
     *
     * @param key Table key.
     * @param tv Timestamp/version of the change.
     * @param operationType Operation type.
     */
    public MvChangeRecord(MvKey key, Instant tv, OpType operationType) {
        this.key = key;
        this.tv = tv;
        this.operationType = operationType;
        this.imageBefore = YdbStruct.EMPTY;
        this.imageAfter = YdbStruct.EMPTY;
    }

    /**
     * Create a change record with before/after images.
     *
     * @param key Table key.
     * @param tv Timestamp/version of the change.
     * @param operationType Operation type.
     * @param imageBefore Row image before change (may be {@code null}).
     * @param imageAfter Row image after change (may be {@code null}).
     */
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

    /**
     * Operation type of the change.
     */
    public static enum OpType {
        /**
         * Insert or update a row
         */
        UPSERT,
        /**
         * Delete a row
         */
        DELETE
    }

}
