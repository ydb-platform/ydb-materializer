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
 * Unit tests for batch marker exposure on {@link MvApplyTask}.
 *
 * @author zinal
 */
public class MvApplyTaskBatchTest {

    private final MvKeyInfo keyInfo = MvTableInfo.newBuilder("t")
            .addColumn("id", PrimitiveType.Int32)
            .addKey("id")
            .build()
            .getKeyInfo();

    @Test
    public void isBatchDelegatesToChangeRecord() {
        MvKey key = new MvKey(keyInfo, new Comparable<?>[]{1});
        MvChangeRecord interactive = new MvChangeRecord(key, Instant.now());
        MvChangeRecord batch = interactive.withBatch(true);

        Assertions.assertFalse(new MvApplyTask(interactive, null, List.of(), 0).isBatch());
        Assertions.assertTrue(new MvApplyTask(batch, null, List.of(), 0).isBatch());
    }

}
