package tech.ydb.mv.feeder;

/**
 * The handler to be called when the scan has completed.
 *
 * @author zinal
 */
public interface MvScanCompletion {

    /**
     * Called on scan finish
     *
     * @param incomplete true, if the scan has not been completed fully (e.g. in
     * case of preliminary shutdown), false otherwise
     */
    void onScanComplete(boolean incomplete);

}
