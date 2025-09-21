package tech.ydb.mv.parser;

import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvTableInfo;

/**
 *
 * @author zinal
 */
public class MvDescriberMeta implements MvDescriber {

    private final MvMetadata metadata;

    public MvDescriberMeta(MvMetadata metadata) {
        this.metadata = metadata;
    }

    public MvMetadata getMetadata() {
        return metadata;
    }

    @Override
    public MvTableInfo describeTable(String tabname) {
        MvTableInfo ret = metadata.getTables().get(tabname);
        if (ret == null) {
            throw new IllegalArgumentException("Unknown table: " + tabname);
        }
        return ret;
    }

}
