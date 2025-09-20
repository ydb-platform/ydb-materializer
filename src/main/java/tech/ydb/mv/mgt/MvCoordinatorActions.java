package tech.ydb.mv.mgt;

/**
 * @author Kirill Kurdyukov
 */
public interface MvCoordinatorActions {

    /**
     * Executed on coordinator startup.
     */
    default void onStart() {
        /* noop */
    }

    /**
     * Executed regularly when coordinator is running.
     */
    void onUpdate();

    /**
     * Executed on coordinator shutdown.
     */
    default void onStop() {
        /* noop */
    }

}
