package tech.ydb.mv.svc;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import tech.ydb.mv.data.MvChangesMultiDict;
import tech.ydb.mv.feeder.MvScanCompletion;

/**
 *
 * @author zinal
 */
class MvDictionaryCommitter implements MvScanCompletion {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvDictionaryCommitter.class);

    private final MvDictionaryScan dictScan;
    private final MvChangesMultiDict changes;
    private final int totalCounter;
    private final AtomicInteger scanCounter = new AtomicInteger(0);
    private final AtomicInteger processingCounter = new AtomicInteger(0);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    public MvDictionaryCommitter(MvDictionaryScan dictScan, MvChangesMultiDict changes, int totalCounter) {
        this.dictScan = dictScan;
        this.changes = changes;
        this.totalCounter = totalCounter;
    }

    @Override
    public void onEndScan() {
        int c1 = scanCounter.incrementAndGet();
        int c2 = processingCounter.get();
        LOG.info("Dictionary refresh scan feeder completed for handler `{}`, {}/{} of {}",
                dictScan.getHandler().getName(), c1, c2, totalCounter);
        completeIf(c1, c2);
    }

    @Override
    public void onEndProcessing() {
        int c1 = scanCounter.get();
        int c2 = processingCounter.incrementAndGet();
        LOG.info("Dictionary refresh processing completed for handler `{}`, {}/{} of {}",
                dictScan.getHandler().getName(), c1, c2, totalCounter);
        completeIf(c1, c2);
    }

    private void completeIf(int c1, int c2) {
        if (c1 >= totalCounter && c2 >= totalCounter) {
            if (completed.getAndSet(true)) {
                return; // already completed
            }
            complete();
        }
    }

    private void complete() {
        LOG.info("Updating dictionary scan positions for handler `{}`",
                dictScan.getHandler().getName());
        dictScan.commitAll(changes);
    }

}
