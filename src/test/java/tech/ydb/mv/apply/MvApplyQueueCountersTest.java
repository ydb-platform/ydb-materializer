package tech.ydb.mv.apply;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for apply-queue size accounting.
 *
 * @author zinal
 */
public class MvApplyQueueCountersTest {

    @Test
    public void tracksInteractiveAndBatchSeparately() {
        MvApplyQueueCounters counters = new MvApplyQueueCounters();

        counters.increment(false);
        counters.increment(true);
        counters.increment(true);

        Assertions.assertEquals(3, counters.getQueueSize());
        Assertions.assertEquals(2, counters.getBatchQueueSize());

        counters.decrement(3, 2);

        Assertions.assertEquals(0, counters.getQueueSize());
        Assertions.assertEquals(0, counters.getBatchQueueSize());
    }

    @Test
    public void drainLeavesRemainingBatchCountIntact() {
        MvApplyQueueCounters counters = new MvApplyQueueCounters();
        for (int i = 0; i < 5; ++i) {
            counters.increment(true);
        }
        counters.increment(false);

        // Simulate one worker drain that took 2 batch + 1 interactive.
        counters.decrement(3, 2);

        Assertions.assertEquals(3, counters.getQueueSize());
        Assertions.assertEquals(3, counters.getBatchQueueSize());
    }

    @Test
    public void clampsCountersWhenDecrementGoesBelowZero() {
        MvApplyQueueCounters counters = new MvApplyQueueCounters();
        counters.increment(true);

        counters.decrement(5, 5);

        Assertions.assertEquals(0, counters.getQueueSize());
        Assertions.assertEquals(0, counters.getBatchQueueSize());
    }

    @Test
    public void supportsForceStyleOverflowAbovePolicyCap() {
        MvApplyQueuePolicy policy = new MvApplyQueuePolicy(1000, 40);
        MvApplyQueueCounters counters = new MvApplyQueueCounters();

        for (int i = 0; i < policy.getMaxBatchQueue(); ++i) {
            Assertions.assertTrue(policy.canAdmit(true,
                    counters.getQueueSize(), counters.getBatchQueueSize()));
            counters.increment(true);
        }

        Assertions.assertFalse(policy.canAdmit(true,
                counters.getQueueSize(), counters.getBatchQueueSize()));

        // Forced fan-out may still enqueue and must keep counters consistent.
        counters.increment(true);
        Assertions.assertEquals(policy.getMaxBatchQueue() + 1, counters.getQueueSize());
        Assertions.assertEquals(policy.getMaxBatchQueue() + 1, counters.getBatchQueueSize());

        // Interactive work remains admissible while total is below queueLimit.
        Assertions.assertTrue(policy.canAdmit(false,
                counters.getQueueSize(), counters.getBatchQueueSize()));
        counters.increment(false);
        Assertions.assertEquals(policy.getMaxBatchQueue() + 2, counters.getQueueSize());
        Assertions.assertEquals(policy.getMaxBatchQueue() + 1, counters.getBatchQueueSize());
    }

    @Test
    public void interactiveRemainsAdmissibleWhenBatchIsAtCap() {
        MvApplyQueuePolicy policy = new MvApplyQueuePolicy(1000, 40);
        MvApplyQueueCounters counters = new MvApplyQueueCounters();

        for (int i = 0; i < policy.getMaxBatchQueue(); ++i) {
            counters.increment(true);
        }

        Assertions.assertFalse(policy.canAdmit(true,
                counters.getQueueSize(), counters.getBatchQueueSize()));
        Assertions.assertTrue(policy.canAdmit(false,
                counters.getQueueSize(), counters.getBatchQueueSize()));

        int reserved = policy.getQueueLimit() - policy.getMaxBatchQueue();
        for (int i = 0; i < reserved; ++i) {
            Assertions.assertTrue(policy.canAdmit(false,
                    counters.getQueueSize(), counters.getBatchQueueSize()));
            counters.increment(false);
        }

        Assertions.assertEquals(policy.getQueueLimit(), counters.getQueueSize());
        Assertions.assertFalse(policy.canAdmit(false,
                counters.getQueueSize(), counters.getBatchQueueSize()));
        Assertions.assertFalse(policy.canAdmit(true,
                counters.getQueueSize(), counters.getBatchQueueSize()));
    }

}
