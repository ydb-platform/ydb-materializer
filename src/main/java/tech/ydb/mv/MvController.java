package tech.ydb.mv;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.feeder.MvFeeder;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;

/**
 * The controller logic for a single handler.
 * Combines the topic reader, apply manager and the required settings.
 *
 * @author zinal
 */
public class MvController {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvController.class);

    private final MvJobContext context;
    private final MvApplyManager applyManager;
    private final MvFeeder feeder;

    public MvController(YdbConnector connector, MvHandler metadata,
            MvHandlerSettings settings) {
        this.context = new MvJobContext(metadata, connector, settings);
        this.applyManager = new MvApplyManager(this.context);
        this.feeder = new MvFeeder(this.context, this.applyManager);
    }

    @Override
    public String toString() {
        return "MvController{" + context.getMetadata().getName() + '}';
    }

    public String getName() {
        return context.getMetadata().getName();
    }

    public MvJobContext getContext() {
        return context;
    }

    public MvApplyManager getApplyManager() {
        return applyManager;
    }

    public MvFeeder getFeeder() {
        return feeder;
    }

    public boolean isRunning() {
        return context.isRunning();
    }

    public boolean isLocked() {
        return applyManager.isLocked();
    }

    public synchronized void start() {
        if (context.isRunning()) {
            LOG.warn("Ignored start call for an already running controller {}", getName());
            return;
        }
        LOG.info("Starting the controller {}", getName());
        // TODO: acquire the global lock
        context.start();
        applyManager.start();
        feeder.start();
    }

    public synchronized void stop() {
        if (context.isRunning()) {
            LOG.warn("Ignored stop call for an already stopped controller {}", getName());
            return;
        }
        LOG.info("Stopping the controller {}", getName());
        context.stop();
        // no explicit stop for applyManager - threads are stopped by context
        feeder.stop();
        // TODO: release the global lock
    }

}
