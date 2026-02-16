package tech.ydb.mv.feeder;

/**
 * The handler to be called when the scan has completed.
 *
 * @author zinal
 */
public interface MvScanCompletion {

    /**
     * Called on scan feeder finish.
     */
    void onEndScan();

    /**
     * Called after the processing of the last scanned record is complete.
     */
    void onEndProcessing();

}
