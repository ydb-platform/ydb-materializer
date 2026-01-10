package tech.ydb.mv.svc;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.apply.MvApplyActionList;
import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.data.MvChangesMultiDict;
import tech.ydb.mv.feeder.MvCdcFeeder;
import tech.ydb.mv.feeder.MvScanCompletion;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.support.MvScanAdapter;
import tech.ydb.mv.support.MvScanDao;

/**
 * The controller logic for a single handler. Combines the topic reader, apply
 * manager and the required settings.
 *
 * @author zinal
 */
public class MvJobController implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvJobController.class);

    private final MvJobContext context;
    private final MvApplyManager applyManager;
    private final MvCdcFeeder cdcFeeder;
    private final AtomicReference<ScheduledFuture<?>> dictCheckFuture = new AtomicReference<>();
    private final AtomicLong dictCheckTime = new AtomicLong(0);

    public MvJobController(MvService service, MvMetadata metadata,
            MvHandler handler, MvHandlerSettings settings) {
        this.context = new MvJobContext(service, metadata, handler, settings);
        this.applyManager = new MvApplyManager(this.context);
        this.cdcFeeder = new MvCdcFeeder(this.context, service.getYdb(), this.applyManager);
    }

    @Override
    public String toString() {
        return "MvController{" + context.getHandler().getName() + '}';
    }

    public String getName() {
        return context.getHandler().getName();
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

    public boolean start() {
        if (context.isRunning()) {
            LOG.warn("Ignored start call for an already running controller `{}`", getName());
            return false;
        }
        if (!obtainLock()) {
            throw new RuntimeException("Failed to obtain the lock for handler `"
                    + getName() + "`, concurrent instance is probably running");
        }
        LOG.info("Starting the controller `{}`", getName());
        context.setStarted();
        applyManager.refreshSelectors(context.getYdb().getTableClient());
        // Temporary workaround: clear the scan positions for the targets,
        // as we cannot resume any regular scans.
        clearScanPositions();
        applyManager.start();
        cdcFeeder.start();
        scheduleDictionaryChecks();
        return true;
    }

    public boolean stop() {
        if (!context.isRunning()) {
            LOG.warn("Ignored stop call for an already stopped controller `{}`", getName());
            return false;
        }
        LOG.info("Stopping the controller `{}`", getName());
        context.setStopped();
        cancelDictionaryChecks();
        cdcFeeder.stop();
        // no explicit stop for applyManager - threads are stopped by context flag
        applyManager.awaitTermination(Duration.ofSeconds(10));
        releaseLock();
        return true;
    }

    @Override
    public void close() {
        if (context.isRunning()) {
            stop();
        }
        cdcFeeder.close();
    }

    public boolean startScan(String name, MvScanSettings settings) {
        var view = context.getHandler().getView(name);
        if (view == null) {
            throw new IllegalArgumentException("Illegal target name `" + name
                    + "` for handler `" + context.getHandler().getName() + "`");
        }
        int counter = 0;
        for (var target : view.getParts().values()) {
            if (context.startScan(target, settings, applyManager)) {
                counter += 1;
            }
        }
        return (counter > 0);
    }

    public boolean stopScan(String name) {
        var view = context.getHandler().getView(name);
        if (view == null) {
            return false;
        }
        int counter = 0;
        for (var target : view.getParts().values()) {
            if (context.stopScan(target)) {
                counter += 1;
            }
        }
        return (counter > 0);
    }

    private boolean obtainLock() {
        if (!context.getService().getLocker().lock(getName())) {
            LOG.warn("Failed to obtain the lock for `{}`, refusing to start", getName());
            return false;
        }
        return true;
    }

    private boolean releaseLock() {
        return context.getService().getLocker().release(getName());
    }

    private void clearScanPositions() {
        var scanDao = new MvScanDao(context.getYdb(), new TempScanDaoAdapter());
        for (var target : context.getHandler().getViews().values()) {
            scanDao.unregisterSpecificScan(target.getName());
        }
    }

    private void scheduleDictionaryChecks() {
        var f = context.getService().getScheduler().scheduleAtFixedRate(
                this::analyzeDictionaryChecks,
                10,
                10,
                TimeUnit.SECONDS
        );
        f = dictCheckFuture.getAndSet(f);
        if (f != null) {
            f.cancel(true);
        }
    }

    private void cancelDictionaryChecks() {
        var f = dictCheckFuture.getAndSet(null);
        if (f != null) {
            f.cancel(true);
        }
    }

    private void analyzeDictionaryChecks() {
        long tv = dictCheckTime.get();
        long cur = System.currentTimeMillis();
        long millis = 1000L * context.getSettings().getDictionaryScanSeconds();
        if ((cur - tv) >= millis) {
            dictCheckTime.set(cur);
            try {
                performDictionaryChecks();
            } catch (Exception ex) {
                LOG.error("Failed to perform dictionary checks on handler `{}`",
                        context.getHandler().getName(), ex);
            }
        }
    }

    private void performDictionaryChecks() {
        var settings = context.getService().getDictionarySettings();
        var dictScan = new MvDictionaryScan(context.getYdb(),
                context.getDescriber(), context.getHandler(), settings);
        var changes = dictScan.scanAll();
        if (changes.isEmpty()) {
            dictScan.commitAll(changes);
            return;
        }
        var filters = changes.toFilters(context.getHandler());
        if (filters.isEmpty()) {
            // No relevant changes in the dictionaries, so move out.
            dictScan.commitAll(changes);
            return;
        }
        if (context.isAnyScanRunning()) {
            LOG.info("Dictionary refresh delayed on handler `{}` "
                    + "due to already running scans", context.getHandler().getName());
            return;
        }
        var completionHandler = new DictScanComplete(dictScan, changes, filters.size());
        for (var filter : filters) {
            LOG.info("Initiating dictionary refresh scan for target `{}` as {} in handler `{}`",
                    filter.getTarget().getName(), filter.getTarget().getAlias(),
                    context.getHandler().getName());
            var action = applyManager.createFilterAction(filter);
            var actions = new MvApplyActionList(action);
            context.startScan(filter.getTarget(), settings, applyManager,
                    actions, completionHandler);
        }
    }

    static class DictScanComplete implements MvScanCompletion {

        final MvDictionaryScan dictScan;
        final MvChangesMultiDict changes;
        final AtomicInteger counter;

        public DictScanComplete(
                MvDictionaryScan dictScan,
                MvChangesMultiDict changes,
                int counter
        ) {
            this.dictScan = dictScan;
            this.changes = changes;
            this.counter = new AtomicInteger(counter);
        }

        @Override
        public void onScanComplete() {
            if (counter.decrementAndGet() == 0) {
                LOG.info("Updating dictionary scan positions for handler `{}`",
                        dictScan.getHandler().getName());
                dictScan.commitAll(changes);
            }
        }

    }

    class TempScanDaoAdapter implements MvScanAdapter {

        final String controlTable;

        TempScanDaoAdapter() {
            this.controlTable = context.getService().getYdb().getProperty(
                    MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        }

        @Override
        public MvTableInfo getTableInfo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getControlTable() {
            return controlTable;
        }

        @Override
        public String getJobName() {
            return getName();
        }

        @Override
        public String getTableName() {
            throw new UnsupportedOperationException();
        }

    }

}
