package tech.ydb.mv;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.feeder.MvCdcFeeder;
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
    private final MvCdcFeeder feeder;

    public MvController(MvService service, MvHandler metadata, MvHandlerSettings settings) {
        this.context = new MvJobContext(service, metadata, settings);
        this.applyManager = new MvApplyManager(this.context);
        this.feeder = new MvCdcFeeder(this.context, this.applyManager);
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

    public MvCdcFeeder getFeeder() {
        return feeder;
    }

    public boolean isRunning() {
        return context.isRunning();
    }

    public boolean isLocked() {
        return applyManager.isLocked();
    }

    public synchronized boolean start() {
        if (context.isRunning()) {
            LOG.warn("Ignored start call for an already running controller {}", getName());
            return false;
        }
        LOG.info("Starting the controller {}", getName());
        if (!context.getService().getCoordinator().lock(getName())) {
            LOG.warn("Failed to obtain the lock for {}, refusing to start", getName());
            return false;
        }
        context.start();
        applyManager.refreshSelectors(context.getYdb().getTableClient());
        applyManager.start();
        feeder.start();
        return true;
    }

    public synchronized boolean stop() {
        if (! context.isRunning()) {
            LOG.warn("Ignored stop call for an already stopped controller {}", getName());
            return false;
        }
        LOG.info("Stopping the controller {}", getName());
        context.stop();
        // no explicit stop for applyManager - threads are stopped by context
        feeder.stop();
        context.getService().getCoordinator().release(getName());
        return true;
    }

}
