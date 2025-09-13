package tech.ydb.mv.model;

/**
 * @author Kirill Kurdyukov
 */
public class MvCoordinatorSettings {

    private final int watchStateDelaySeconds;

    public MvCoordinatorSettings() {
        this.watchStateDelaySeconds = 5;
    }

    public MvCoordinatorSettings(int watchStateDelaySeconds) {
        this.watchStateDelaySeconds = watchStateDelaySeconds;
    }

    public int getWatchStateDelaySeconds() {
        return watchStateDelaySeconds;
    }
}
