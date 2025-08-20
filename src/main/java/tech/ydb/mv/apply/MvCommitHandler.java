package tech.ydb.mv.apply;

/**
 * The handler for the commit action to be executed when the record is processed.
 *
 * @author zinal
 */
public interface MvCommitHandler {

    /**
     * Apply the commit for the specified number of records.
     * @param count Number of records processed.
     */
    void apply(int count);

}
