package tech.ydb.mv;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.apply.MvApplyManager;
import tech.ydb.mv.feeder.MvCdcAdapter;
import tech.ydb.mv.feeder.MvScanFeeder;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvTarget;

/**
 * Contextual data for running a job processing a single handler.
 *
 * @author zinal
 */
public class MvJobContext implements MvCdcAdapter  {

    private final MvService service;
    private final MvHandler metadata;
    private final MvHandlerSettings settings;
    // initially stopped
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);
    // target name -> scan feeder
    private final HashMap<String, MvScanFeeder> scanFeeders = new HashMap<>();

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

    public YdbConnector getYdb() {
        return service.getYdb();
    }

    public MvHandlerSettings getSettings() {
        return settings;
    }

    @Override
    public boolean isRunning() {
        return shouldRun.get();
    }

    public void start() {
        shouldRun.set(true);
    }

    public void stop() {
        shouldRun.set(false);
    }

    @Override
    public String getFeederName() {
        return metadata.getName();
    }

    @Override
    public int getCdcReaderThreads() {
        return settings.getCdcReaderThreads();
    }

    @Override
    public String getConsumerName() {
        return metadata.getConsumerNameAlways();
    }

    public synchronized void startScan(MvTarget target, MvScanSettings settings,
            MvApplyManager applyManager) {
        if ( target == null ||
                metadata.getTargets().get(target.getName()) != target) {
            throw new IllegalArgumentException("Illegal target `" + target
                    + "` for handler `" + metadata.getName() + "`");
        }
        if (! isRunning()) {
            throw new IllegalStateException("Scan start refused on stopped handler `"
                    + metadata.getName() + "`");
        }
        MvScanFeeder sf = scanFeeders.get(target.getName());
        if (sf == null) {
            sf = new MvScanFeeder(this, applyManager, target, settings);
            scanFeeders.put(target.getName(), sf);
        }
        sf.start();
    }

    public synchronized boolean stopScan(MvTarget target) {
        if ( target == null ||
                metadata.getTargets().get(target.getName()) != target) {
            return false;
        }
        MvScanFeeder sf = scanFeeders.remove(target.getName());
        if (sf == null) {
            return false;
        }
        sf.stop();
        return true;
    }

    public synchronized void completeScan(MvTarget target) {
        if (target!=null) {
            scanFeeders.remove(target.getName());
        }
    }

}
