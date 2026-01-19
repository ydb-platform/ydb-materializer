package tech.ydb.mv.svc;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.apply.MvApplyActionList;
import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.feeder.MvCdcAdapter;
import tech.ydb.mv.feeder.MvScanCompletion;
import tech.ydb.mv.feeder.MvScanFeeder;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvDescriberMeta;

/**
 * Contextual data for running a job processing a single handler.
 *
 * @author zinal
 */
public class MvJobContext implements MvCdcAdapter {

    private final MvService service;
    private final MvHandler handler;
    private final MvHandlerSettings settings;
    private final MvDescriberMeta describer;
    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);
    // target -> scan feeder
    private final HashMap<MvViewExpr, MvScanFeeder> scanFeeders = new HashMap<>();

    public MvJobContext(MvService service, MvMetadata metadata,
            MvHandler handler, MvHandlerSettings settings) {
        this.service = service;
        this.handler = handler;
        this.settings = settings;
        this.describer = new MvDescriberMeta(metadata);
    }

    @Override
    public String toString() {
        return "MvJobContext{" + handler.getName() + '}';
    }

    public MvService getService() {
        return service;
    }

    public MvHandler getHandler() {
        return handler;
    }

    public YdbConnector getYdb() {
        return service.getYdb();
    }

    public MvHandlerSettings getSettings() {
        return settings;
    }

    public MvDescriberMeta getDescriber() {
        return describer;
    }

    @Override
    public boolean isRunning() {
        return shouldRun.get();
    }

    public void setStarted() {
        shouldRun.set(true);
    }

    public void setStopped() {
        shouldRun.set(false);
    }

    @Override
    public String getFeederName() {
        return handler.getName();
    }

    @Override
    public int getCdcReaderThreads() {
        return settings.getCdcReaderThreads();
    }

    @Override
    public String getConsumerName() {
        return handler.getConsumerNameAlways();
    }

    public synchronized boolean isAnyScanRunning() {
        for (var sf : scanFeeders.values()) {
            if (sf.isRunning()) {
                return true;
            }
        }
        return false;
    }

    public boolean startScan(MvViewExpr target, MvScanSettings settings,
            MvApplyManager applyManager) {
        return startScan(target, settings, applyManager, null, null);
    }

    public synchronized boolean startScan(MvViewExpr target, MvScanSettings settings,
            MvApplyManager applyManager, MvApplyActionList actions, MvScanCompletion completion) {
        if (target == null || !handler.containsPart(target)) {
            throw new IllegalArgumentException("Illegal target `" + target
                    + "` for handler `" + handler.getName() + "`");
        }
        if (!isRunning()) {
            throw new IllegalStateException("Scan start refused on stopped handler `"
                    + handler.getName() + "`");
        }
        MvScanFeeder sf = scanFeeders.get(target);
        if (sf != null && sf.isRunning()) {
            return false;
        }
        sf = new MvScanFeeder(this, applyManager, target, settings, actions, completion);
        scanFeeders.put(target, sf);
        return sf.start();
    }

    public synchronized boolean stopScan(MvViewExpr target) {
        if (target == null || !handler.containsPart(target)) {
            return false;
        }
        MvScanFeeder sf = scanFeeders.remove(target);
        if (sf == null) {
            return false;
        }
        return sf.stop();
    }

    public synchronized void forgetScan(MvViewExpr target) {
        if (target != null) {
            scanFeeders.remove(target);
        }
    }

}
