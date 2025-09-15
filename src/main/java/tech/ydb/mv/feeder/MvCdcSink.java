package tech.ydb.mv.feeder;

import java.util.Collection;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvTarget;

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
     * @param target Perform refresh of the specified target only. null otherwise
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    boolean submit(MvTarget target, Collection<MvChangeRecord> records, MvCommitHandler handler);

    /**
     * Forcibly insert the input data to the queue of the proper workers.
     * May overflow the expected size of the queues.
     *
     * @param target Perform refresh of the specified target only. null otherwise
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     */
    void submitForce(MvTarget target, Collection<MvChangeRecord> records, MvCommitHandler handler);

}
