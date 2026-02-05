package tech.ydb.mv.feeder;

/**
 * Adapter interface for CDC (change data capture) sources feeding changes into
 * the materializer.
 *
 * @author zinal
 */
public interface MvCdcAdapter {

    /**
     * Get the name for the handler (job) which runs the CDC feeder.
     *
     * @return Handler (e.g. job) name for the CDC feeder.
     */
    String getFeederName();

    /**
     * Get number of reader threads.
     *
     * @return Number of reader threads used to consume CDC events.
     */
    int getCdcReaderThreads();

    /**
     * Get consumer name.
     *
     * @return Consumer name to be used for CDC stream/topic subscription.
     */
    String getConsumerName();

    /**
     * Check whether adapter is running.
     *
     * @return {@code true} if the adapter is currently running (actively
     * consuming changes).
     */
    boolean isRunning();

}
