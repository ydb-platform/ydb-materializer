package tech.ydb.mv.feeder;

/**
 * The handler for the commit action to be executed when the record is processed.
 *
 * @author zinal
 */
public interface MvCommitHandler {

    /**
     * Apply the commit for the specified number of records.
     *
     * @param count Number of records processed (positive), or number of extra operations pending on the records (negative).
     */
    void apply(int count);

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

}
