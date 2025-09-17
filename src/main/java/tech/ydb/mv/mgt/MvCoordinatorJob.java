package tech.ydb.mv.mgt;

/**
 * @author Kirill Kurdyukov
 */
public interface MvCoordinatorJob {

    void start();

    void performCoordinationTask();

    void stop();
}
