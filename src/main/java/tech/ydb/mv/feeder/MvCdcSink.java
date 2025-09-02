package tech.ydb.mv.feeder;

import java.util.Collection;

import tech.ydb.mv.model.MvChangeRecord;

/**
 *
 * @author zinal
 */
public interface MvCdcSink {

    boolean submit(Collection<MvChangeRecord> records, MvCommitHandler handler);

}
