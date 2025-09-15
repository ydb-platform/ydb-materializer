package tech.ydb.mv.mgt;

/**
 * @author Kirill Kurdyukov
 */
public interface MvCoordinatorJob {

    void performCoordinationTask();

    void stopJobs();
}
