package tech.ydb.mv.feeder;

/**
 * The handler for the commit action to be executed when the record is
 * processed.
 *
 * @author zinal
 */
public interface MvCommitHandler {

    /**
     * @return Unique instance identifier for this handler within its type.
     */
    long getInstance();

    /**
     * @return Current commit pending counter.
     */
    int getCounter();

    /**
     * Apply the commit for the specified number of records.
     *
     * @param count Number of records processed (positive), or zero to just
     * reinforce the commit operation.
     */
    void commit(int count);

    /**
     * Reserve the extra number of records to be processed.
     *
     * @param count Number of records to be reserved (positive).
     */
    void reserve(int count);

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

}
