package tech.ydb.mv;

import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.feeder.MvCdcReader;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;

/**
 * The controller logic for a single handler.
 * Combines the topic reader, apply manager, ...
 *
 * @author zinal
 */
public class MvHandlerController {

    private final YdbConnector connector;
    private final MvHandler metadata;
    private final MvHandlerSettings settings;
    private final MvApplyManager applyManager;
    private final MvCdcReader cdcReader;

    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    public MvHandlerController(YdbConnector connector, MvHandler metadata,
            MvHandlerSettings settings) {
        this.connector = connector;
        this.metadata = metadata;
        this.settings = settings;
        this.applyManager = new MvApplyManager(this);
        this.cdcReader = new MvCdcReader(this);
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvHandler getMetadata() {
        return metadata;
    }

    public MvHandlerSettings getSettings() {
        return settings;
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public MvCdcReader getCdcReader() {
        return cdcReader;
    }

    public boolean isRunning() {
        return shouldRun.get();
    }

    public void start() {
        shouldRun.set(true);
        cdcReader.start();
    }

    public void stop() {
        shouldRun.set(false);
        cdcReader.stop();
    }

}
