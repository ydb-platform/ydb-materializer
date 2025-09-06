package tech.ydb.mv.feeder;

import java.util.Collection;

import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvInput;

/**
 *
 * @author zinal
 */
public interface MvCdcSink {

    /**
     * @return The input descriptions for the current sink.
     */
    Collection<MvInput> getInputs();

    /**
     * Insert the input data to the queues of the proper workers.
     *
     * In case some of the queues are full, wait until the capacity
     * becomes available, or until the update manager is stopped.
     *
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    boolean submit(Collection<MvChangeRecord> records, MvCommitHandler handler);

    /**
     * Forcibly insert the input data to the queue of the proper workers.
     * May overflow the expected size of the queues.
     *
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     */
    void submitForce(Collection<MvChangeRecord> records, MvCommitHandler handler);

}
