package tech.ydb.mv.feeder;

import java.util.Collection;
import tech.ydb.mv.apply.MvApplyActionList;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public interface MvSink {

    /**
     * @return The input descriptions for the current sink.
     */
    Collection<MvInput> getInputs();

    /**
     * Insert the input data to the queues of the proper workers.
     *
     * In case some of the queues are full, wait until the capacity becomes
     * available, or until the update manager is stopped.
     *
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    boolean submit(Collection<MvChangeRecord> records, MvCommitHandler handler);

    /**
     * Same as submit(), but only activates the refresh actions for the specific
     * target.
     *
     * @param target Perform refresh of the specified target only. null
     * otherwise
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    default boolean submitRefresh(MvTarget target, Collection<MvChangeRecord> records, MvCommitHandler handler) {
        return submit(records, handler);
    }

    /**
     * Same as submit(), but only activates the specified actions.
     *
     * In case some of the queues are full, wait until the capacity becomes
     * available, or until the update manager is stopped.
     *
     * @param actions The explicit list of actions to be executed.
     * @param records The change records to be submitted for processing.
     * @param handler The commit processing handler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    default boolean submitCustom(MvApplyActionList actions, Collection<MvChangeRecord> records, MvCommitHandler handler) {
        return submit(records, handler);
    }

}
