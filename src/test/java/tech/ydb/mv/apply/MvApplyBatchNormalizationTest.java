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
 * Unit tests for batch detection/normalization used by {@link MvApplyManager}.
 *
 * @author zinal
 */
public class MvApplyBatchNormalizationTest {

    private final MvKeyInfo keyInfo = MvTableInfo.newBuilder("t")
            .addColumn("id", PrimitiveType.Int32)
            .addKey("id")
            .build()
            .getKeyInfo();

    @Test
    public void detectBatchSubmissionRequiresAtLeastOneBatchRecord() {
        MvChangeRecord interactive = record(1, false);
        MvChangeRecord batch = record(2, true);

        Assertions.assertFalse(MvApplyManager.detectBatchSubmission(List.of()));
        Assertions.assertFalse(MvApplyManager.detectBatchSubmission(List.of(interactive)));
        Assertions.assertTrue(MvApplyManager.detectBatchSubmission(List.of(batch)));
        Assertions.assertTrue(MvApplyManager.detectBatchSubmission(
                List.of(interactive, batch)));
    }

    @Test
    public void normalizeBatchFlagMarksWholeSubmission() {
        MvChangeRecord interactive = record(1, false);
        MvChangeRecord batch = record(2, true);

        List<MvChangeRecord> asBatch = MvApplyManager.normalizeBatchFlag(
                List.of(interactive, batch), true);
        Assertions.assertEquals(2, asBatch.size());
        Assertions.assertTrue(asBatch.get(0).isBatch());
        Assertions.assertTrue(asBatch.get(1).isBatch());
        Assertions.assertFalse(interactive.isBatch());

        List<MvChangeRecord> asInteractive = MvApplyManager.normalizeBatchFlag(
                List.of(batch), false);
        Assertions.assertEquals(1, asInteractive.size());
        Assertions.assertFalse(asInteractive.get(0).isBatch());
        Assertions.assertTrue(batch.isBatch());
    }

    @Test
    public void mixedSubmissionFollowsDetectThenNormalizePath() {
        List<MvChangeRecord> input = List.of(record(1, false), record(2, true), record(3, false));
        boolean batch = MvApplyManager.detectBatchSubmission(input);
        List<MvChangeRecord> normalized = MvApplyManager.normalizeBatchFlag(input, batch);

        Assertions.assertTrue(batch);
        Assertions.assertTrue(normalized.stream().allMatch(MvChangeRecord::isBatch));
    }

    private MvChangeRecord record(int id, boolean batch) {
        MvKey key = new MvKey(keyInfo, new Comparable<?>[]{id});
        return new MvChangeRecord(key, Instant.parse("2026-01-01T00:00:00Z"))
                .withBatch(batch);
    }

}
