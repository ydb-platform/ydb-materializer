package tech.ydb.mv.svc;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.feeder.MvCdcFeeder;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvTarget;

/**
 * The controller logic for a single handler. Combines the topic reader, apply
 * manager and the required settings.
 *
 * @author zinal
 */
public class MvJobController {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvJobController.class);

    private final MvJobContext context;
    private final MvApplyManager applyManager;
    private final MvCdcFeeder cdcFeeder;

    public MvJobController(MvService service, MvHandler metadata, MvHandlerSettings settings) {
        this.context = new MvJobContext(service, metadata, settings);
        this.applyManager = new MvApplyManager(this.context);
        this.cdcFeeder = new MvCdcFeeder(this.context, service.getYdb(), this.applyManager);
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

    public MvCdcFeeder getCdcFeeder() {
        return cdcFeeder;
    }

    public boolean isRunning() {
        return context.isRunning();
    }

    public boolean isLocked() {
        return applyManager.isLocked();
    }

    public synchronized boolean start() {
        if (context.isRunning()) {
            LOG.warn("Ignored start call for an already running controller `{}`", getName());
            return false;
        }
        if (!context.getService().getLocker().lock(getName())) {
            LOG.warn("Failed to obtain the lock for `{}`, refusing to start", getName());
            return false;
        }
        LOG.info("Starting the controller `{}`", getName());
        context.start();
        applyManager.refreshSelectors(context.getYdb().getTableClient());
        applyManager.start();
        cdcFeeder.start();
        return true;
    }

    public synchronized boolean stop() {
        if (!context.isRunning()) {
            LOG.warn("Ignored stop call for an already stopped controller `{}`", getName());
            return false;
        }
        LOG.info("Stopping the controller `{}`", getName());
        context.stop();
        // no explicit stop for applyManager - threads are stopped by context
        cdcFeeder.stop();
        context.getService().getLocker().release(getName());
        return true;
    }

    public boolean startScan(String name, MvScanSettings settings) {
        MvTarget target = context.getMetadata().getTarget(name);
        if (target == null) {
            throw new IllegalArgumentException("Illegal target name `" + name
                    + "` for handler `" + context.getMetadata().getName() + "`");
        }
        return context.startScan(target, settings, applyManager);
    }

    public boolean stopScan(String name) {
        MvTarget target = context.getMetadata().getTarget(name);
        if (target == null) {
            return false;
        }
        return context.stopScan(target);
    }

}
