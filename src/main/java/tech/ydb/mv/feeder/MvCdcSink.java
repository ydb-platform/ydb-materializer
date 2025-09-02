package tech.ydb.mv.feeder;

import java.util.Collection;

import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvInput;

/**
 *
 * @author zinal
 */
public interface MvCdcSink {

    Collection<MvInput> getInputs();

    boolean submit(Collection<MvChangeRecord> records, MvCommitHandler handler);

}
