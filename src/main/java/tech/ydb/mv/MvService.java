package tech.ydb.mv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.batch.MvDictionaryManager;
import tech.ydb.mv.dist.MvLocker;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.parser.MvMetadataReader;
import tech.ydb.mv.support.MvConfigReader;
import tech.ydb.mv.support.MvIssuePrinter;
import tech.ydb.mv.support.MvSqlPrinter;
import tech.ydb.mv.support.YdbMisc;

/**
 * Work context for YDB Materializer activities.
 *
 * @author zinal
 */
public class MvService implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvService.class);

    private final YdbConnector ydb;
    private final MvMetadata metadata;
    private final MvLocker locker;
    private final AtomicReference<MvHandlerSettings> handlerSettings;
    private final AtomicReference<MvDictionarySettings> dictionarySettings;
    private volatile MvDictionaryManager dictionaryManager = null;
    private final HashMap<String, MvJobController> handlers = new HashMap<>();
    private final RefreshTask refreshTask = new RefreshTask();
    private volatile Thread refreshThread = null;

    public MvService(YdbConnector ydb) {
        this.ydb = ydb;
        this.metadata = MvConfigReader.read(ydb, ydb.getConfig().getProperties());
        this.locker = new MvLocker(ydb);
        this.handlerSettings = new AtomicReference<>(new MvHandlerSettings());
        this.dictionarySettings = new AtomicReference<>(new MvDictionarySettings());
        refreshMetadata();
    }

    public YdbConnector getYdb() {
        return ydb;
    }

    public MvMetadata getMetadata() {
        return metadata;
    }

    public MvLocker getLocker() {
        return locker;
    }

    public MvHandlerSettings getHandlerSettings() {
        return new MvHandlerSettings(handlerSettings.get());
    }

    public void setHandlerSettings(MvHandlerSettings defaultSettings) {
        if (defaultSettings==null) {
            defaultSettings = new MvHandlerSettings();
        } else {
            defaultSettings = new MvHandlerSettings(defaultSettings);
        }
        this.handlerSettings.set(defaultSettings);
    }

    public MvDictionarySettings getDictionarySettings() {
        return new MvDictionarySettings(dictionarySettings.get());
    }

    public void setDictionarySettings(MvDictionarySettings defaultSettings) {
        if (defaultSettings==null) {
            defaultSettings = new MvDictionarySettings();
        } else {
            defaultSettings = new MvDictionarySettings(defaultSettings);
        }
        this.dictionarySettings.set(defaultSettings);
    }

    public void applyDefaults() {
        var props = ydb.getConfig().getProperties();
        setHandlerSettings(new MvHandlerSettings(props));
        setDictionarySettings(new MvDictionarySettings(props));
    }

    /**
     * @return true, if at least one handler is active, and false otherwise.
     */
    public synchronized boolean isRunning() {
        return !handlers.isEmpty() || (dictionaryManager != null);
    }

    /**
     * Stop all the handlers which are running.
     */
    public synchronized void shutdown() {
        handlers.values().forEach(h -> h.stop());
        handlers.clear();
        locker.releaseAll();
        if (dictionaryManager != null) {
            dictionaryManager.stop();
            dictionaryManager = null;
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    public synchronized void startDictionaryHandler() {
        if (dictionaryManager != null) {
            return;
        }
        dictionaryManager = new MvDictionaryManager(metadata, ydb, dictionarySettings.get());
        dictionaryManager.start();
    }

    public synchronized void stopDictionaryHandler() {
        if (dictionaryManager != null) {
            dictionaryManager.stop();
            dictionaryManager = null;
        }
    }

    /**
     * Start the handler with the specified name, using the current default settings.
     * @param name Name of the handler to be started
     * @return true, if handler has been started, false otherwise
     */
    public boolean startHandler(String name) {
        return startHandler(name, getHandlerSettings());
    }

    /**
     * Start the handler with the specified settings.
     *
     * @param name Name of the handler to be started
     * @param settings The settings to be used by the handler
     * @return true, if handler has been started, false otherwise
     */
    public synchronized boolean startHandler(String name, MvHandlerSettings settings) {
        if (refreshThread==null || !refreshThread.isAlive()) {
            refreshThread = new Thread(refreshTask);
            refreshThread.setName("MvServiceRefreshThread");
            refreshThread.setDaemon(true);
            refreshThread.start();
        }
        MvJobController c = handlers.get(name);
        if (c==null) {
            MvHandler handler = metadata.getHandlers().get(name);
            if (handler==null) {
                throw new IllegalArgumentException("Unknown handler name: " + name);
            }
            c = new MvJobController(this, handler, settings);
            handlers.put(name, c);
        }
        return c.start();
    }

    /**
     * Stop the handler with the specified name.
     * @param name The name of the handler to be stopped
     * @return true, if the handler was actually stopped, and false otherwise
     */
    public synchronized boolean stopHandler(String name) {
        MvJobController c = handlers.remove(name);
        if (c == null) {
            return false;
        }
        c.stop();
        return true;
    }

    /**
     * Start the full scan for the specified target in  the specified handler.For illegal arguments, exceptions are thrown.
     *
     * @param handlerName Name of the handler
     * @param targetName Name of the target
     * @param settings Settings for the specific scan
     */
    public synchronized void startScan(String handlerName, String targetName,
            MvScanSettings settings) {
        if (settings==null) {
            settings = new MvScanSettings(ydb.getConfig().getProperties());
        }
        MvJobController c = handlers.get(handlerName);
        if (c == null) {
            throw new IllegalArgumentException("Unknown handler name: " + handlerName);
        }
        c.startScan(targetName, settings);
    }

    /**
     * Stops the full scan for the specified target in the specified handler.
     * For illegal arguments, false is returned.
     *
     * @param handlerName Name of the handler
     * @param targetName Name of the target
     * @return true, if the scan was started, false otherwise
     */
    public synchronized boolean stopScan(String handlerName, String targetName) {
        MvJobController c = handlers.get(handlerName);
        if (c == null) {
            return false;
        }
        return c.stopScan(targetName);
    }

    /**
     * Print the list of issues in the current context to stdout.
     */
    public void printIssues() {
        new MvIssuePrinter(metadata).write(System.out);
    }

    /**
     * Generate the set of SQL statements and print to stdout.
     */
    public void printSql() {
        new MvSqlPrinter(metadata).write(System.out);
    }

    /**
     * Start the default handlers.
     */
    public void startHandlers() {
        if (LOG.isInfoEnabled()) {
            String msg = new MvIssuePrinter(metadata).write();
            LOG.info("\n"
                    + "---- BEGIN CONTEXT INFO ----\n"
                    + "{}\n"
                    + "----- END CONTEXT INFO -----", msg);
        }
        if (! metadata.isValid()) {
            throw new IllegalStateException(
                    "Refusing to start due to configuration errors.");
        }
        for (String handlerName : parseActiveHandlerNames()) {
            try {
                startHandler(handlerName);
            } catch(Exception ex) {
                LOG.error("Failed to activate the handler {}", handlerName, ex);
            }
        }
    }

    /**
     * Start and run the set of default handlers.
     */
    public void runHandlers() {
        startHandlers();
        while (isRunning()) {
            YdbMisc.sleep(100L);
        }
    }

    private List<String> parseActiveHandlerNames() {
        String v = ydb.getConfig().getProperties().getProperty(MvConfig.CONF_HANDLERS);
        if (v==null) {
            return Collections.emptyList();
        }
        v = v.trim();
        if (v.length()==0) {
            return Collections.emptyList();
        }
        return Arrays.asList(v.split("[,]"));
    }

    private synchronized ArrayList<MvJobController> grabControllers() {
        return new ArrayList<>(handlers.values());
    }

    private void refreshMetadata() {
        if (! metadata.isValid()) {
            LOG.warn("Parser produced errors, metadata retrieval skipped.");
        } else {
            LOG.info("Loading metadata and performing validation...");
            metadata.linkAndValidate(new MvMetadataReader(ydb));
        }
    }

    private void sleepSome() {
        for (int i=0; i<3000; ++i) {
            if (! isRunning()) {
                break;
            }
            YdbMisc.sleep(100L);
        }
    }

    private void refreshSelectors() {
        for (MvJobController c : grabControllers()) {
            c.getApplyManager().refreshSelectors(ydb.getTableClient());
        }
    }

    private class RefreshTask implements Runnable {
        @Override
        public void run() {
            sleepSome();
            while (isRunning()) {
                refreshSelectors();
                sleepSome();
            }
        }
    }
}
