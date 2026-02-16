package tech.ydb.mv.feeder;

/**
 * The handler to be called when the scan has completed.
 *
 * @author zinal
 */
public interface MvScanCompletion {

    /**
     * Called on scan finish
     */
    void onScanComplete();

}
