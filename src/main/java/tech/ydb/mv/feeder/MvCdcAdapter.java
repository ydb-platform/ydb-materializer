package tech.ydb.mv.feeder;

/**
 * Adapter interface for CDC (change data capture) sources feeding changes into the materializer.
 * @author zinal
 */
public interface MvCdcAdapter {

    /**
     * Get a human-readable identifier for this feeder.
     *
     * @return Unique name of this feeder instance (used for logging/identification).
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
     * @return {@code true} if the adapter is currently running (actively consuming changes).
     */
    boolean isRunning();

}
