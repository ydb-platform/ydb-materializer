package tech.ydb.mv.support;

import tech.ydb.mv.model.MvTableInfo;

/**
 *
 * @author zinal
 */
public interface MvScanAdapter {

    MvTableInfo getTableInfo();

    String getControlTable();

    String getHandlerName();

    String getTargetName();

}
