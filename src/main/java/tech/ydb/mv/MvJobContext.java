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

    private final MvService service;
    private final MvHandler metadata;
    private final MvHandlerSettings settings;
    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvJobContext(MvService service, MvHandler metadata, MvHandlerSettings settings) {
        this.service = service;
        this.metadata = metadata;
        this.settings = settings;
    }

    @Override
    public String toString() {
        return "MvJobContext{" + metadata.getName() + '}';
    }

    public MvService getService() {
        return service;
    }

    public MvHandler getMetadata() {
        return metadata;
    }

    public YdbConnector getConnector() {
        return service.getYdb();
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
