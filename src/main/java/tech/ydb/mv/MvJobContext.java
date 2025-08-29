package tech.ydb.mv;

import java.util.concurrent.atomic.AtomicBoolean;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;

/**
 * Contextual data for running a job processing a single handler.
 *
 * @author zinal
 */
public class MvJobContext {

    private final MvHandler metadata;
    private final YdbConnector connector;
    private final MvHandlerSettings settings;
    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvJobContext(MvHandler metadata, YdbConnector connector, MvHandlerSettings settings) {
        this.metadata = metadata;
        this.connector = connector;
        this.settings = settings;
    }

    @Override
    public String toString() {
        return "MvJobContext{" + metadata.getName() + '}';
    }

    public MvHandler getMetadata() {
        return metadata;
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvHandlerSettings getSettings() {
        return settings;
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public void start() {
        shouldRun.set(true);
    }

    public void stop() {
        shouldRun.set(false);
    }

}
