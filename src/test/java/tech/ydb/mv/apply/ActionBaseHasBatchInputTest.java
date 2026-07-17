package tech.ydb.mv.apply;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.table.values.PrimitiveType;

/**
 * Unit tests for {@link ActionBase#hasBatchInput(List)}.
 *
 * @author zinal
 */
public class ActionBaseHasBatchInputTest {

    private final MvKeyInfo keyInfo = MvTableInfo.newBuilder("t")
            .addColumn("id", PrimitiveType.Int32)
            .addKey("id")
            .build()
            .getKeyInfo();

    @Test
    public void hasBatchInputDetectsAnyBatchTask() {
        Assertions.assertFalse(ActionBase.hasBatchInput(List.of()));
        Assertions.assertFalse(ActionBase.hasBatchInput(List.of(task(1, false))));
        Assertions.assertTrue(ActionBase.hasBatchInput(List.of(task(1, true))));
        Assertions.assertTrue(ActionBase.hasBatchInput(
                List.of(task(1, false), task(2, true), task(3, false))));
    }

    @Test
    public void derivedRecordsShouldReuseHasBatchInputResult() {
        // Mirrors Filter/Grab/Transform: one batch input makes all outputs batch.
        boolean batch = ActionBase.hasBatchInput(List.of(task(1, false), task(2, true)));
        MvChangeRecord derived = new MvChangeRecord(
                new MvKey(keyInfo, new Comparable<?>[]{10}),
                Instant.parse("2026-01-01T00:00:00Z"))
                .withBatch(batch);

        Assertions.assertTrue(derived.isBatch());
    }

    private MvApplyTask task(int id, boolean batch) {
        MvKey key = new MvKey(keyInfo, new Comparable<?>[]{id});
        MvChangeRecord record = new MvChangeRecord(key, Instant.parse("2026-01-01T00:00:00Z"))
                .withBatch(batch);
        return new MvApplyTask(record, null, List.of(), 0);
    }

}
