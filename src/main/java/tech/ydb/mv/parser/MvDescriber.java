package tech.ydb.mv.parser;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvTableInfo;

/**
 *
 * @author zinal
 */
public interface MvDescriber {

    MvTableInfo describeTable(String tabname);

    default YdbConnector getYdb() {
        return null;
    }

}
