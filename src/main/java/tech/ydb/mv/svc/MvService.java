package tech.ydb.mv.svc;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.MvApi;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.mgt.MvLocker;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.parser.MvDescriberYdb;
import tech.ydb.mv.support.MvConfigReader;
import tech.ydb.mv.support.MvIssuePrinter;
import tech.ydb.mv.support.MvSqlPrinter;
import tech.ydb.mv.support.YdbMisc;

/**
 * Local management for YDB Materializer activities.
 *
 * @author zinal
 */
public class MvService implements MvApi {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvService.class);

    private final YdbConnector ydb;
    private final MvMetadata metadata;
    private final MvLocker locker;
    private final AtomicReference<MvHandlerSettings> handlerSettings;
    private final AtomicReference<MvDictionarySettings> dictionarySettings;
    private final AtomicReference<MvScanSettings> scanSettings;
    private volatile MvDictionaryLogger dictionaryManager = null;
    private final HashMap<String, MvJobController> handlers = new HashMap<>();
    private final SchedulerTask schedulerTask = new SchedulerTask();
    private volatile Thread schedulerThread = null;

    public MvService(YdbConnector ydb) {
        this.ydb = ydb;
        this.metadata = MvConfigReader.read(ydb, ydb.getConfig().getProperties());
        this.locker = new MvLocker(ydb);
        this.handlerSettings = new AtomicReference<>(new MvHandlerSettings());
        this.dictionarySettings = new AtomicReference<>(new MvDictionarySettings());
        this.scanSettings = new AtomicReference<>(new MvScanSettings());
        refreshMetadata();
    }

    @Override
    public YdbConnector getYdb() {
        return ydb;
    }

    @Override
    public MvMetadata getMetadata() {
        return metadata;
    }

    public MvLocker getLocker() {
        return locker;
    }

    @Override
    public MvHandlerSettings getHandlerSettings() {
        return new MvHandlerSettings(handlerSettings.get());
    }

    @Override
    public void setHandlerSettings(MvHandlerSettings defaultSettings) {
        if (defaultSettings == null) {
            defaultSettings = new MvHandlerSettings();
        } else {
            defaultSettings = new MvHandlerSettings(defaultSettings);
        }
        this.handlerSettings.set(defaultSettings);
    }

    @Override
    public MvDictionarySettings getDictionarySettings() {
        return new MvDictionarySettings(dictionarySettings.get());
    }

    @Override
    public void setDictionarySettings(MvDictionarySettings defaultSettings) {
        if (defaultSettings == null) {
            defaultSettings = new MvDictionarySettings();
        } else {
            defaultSettings = new MvDictionarySettings(defaultSettings);
        }
        this.dictionarySettings.set(defaultSettings);
    }

    @Override
    public MvScanSettings getScanSettings() {
        return new MvScanSettings(scanSettings.get());
    }

    @Override
    public void setScanSettings(MvScanSettings defaultSettings) {
        if (defaultSettings == null) {
            defaultSettings = new MvScanSettings();
        } else {
            defaultSettings = new MvScanSettings(defaultSettings);
        }
        this.scanSettings.set(defaultSettings);
    }

    @Override
    public void applyDefaults(Properties props) {
        if (props == null) {
            props = ydb.getConfig().getProperties();
        }
        setHandlerSettings(new MvHandlerSettings(props));
        setDictionarySettings(new MvDictionarySettings(props));
        setScanSettings(new MvScanSettings(props));
    }

    @Override
    public synchronized boolean isRunning() {
        return !handlers.isEmpty() || (dictionaryManager != null);
    }

    @Override
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

    public synchronized boolean startDictionaryHandler() {
        if (dictionaryManager != null) {
            return false;
        }
        dictionaryManager = new MvDictionaryLogger(metadata, ydb, dictionarySettings.get());
        dictionaryManager.start();
        return true;
    }

    public synchronized boolean stopDictionaryHandler() {
        if (dictionaryManager == null) {
            return false;
        }
        dictionaryManager.stop();
        dictionaryManager = null;
        return true;
    }

    /**
     * Start the handler with the specified name, using the current default
     * settings.
     *
     * @param name Name of the handler to be started
     */
    @Override
    public boolean startHandler(String name) {
        if (MvConfig.HANDLER_DICTIONARY.equalsIgnoreCase(name)) {
            return startDictionaryHandler();
        }
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
        if (schedulerThread == null || !schedulerThread.isAlive()) {
            schedulerThread = new Thread(schedulerTask);
            schedulerThread.setName("mv-scheduler");
            schedulerThread.setDaemon(true);
            schedulerThread.start();
        }
        MvJobController c = handlers.get(name);
        if (c == null) {
            MvHandler handler = metadata.getHandlers().get(name);
            if (handler == null) {
                throw new IllegalArgumentException("Unknown handler name: " + name);
            }
            c = new MvJobController(this, handler, settings);
            handlers.put(name, c);
        }
        return c.start();
    }

    /**
     * Stop the handler with the specified name.
     *
     * @param name The name of the handler to be stopped
     * @return true, if the handler was actually stopped, and false otherwise
     */
    @Override
    public synchronized boolean stopHandler(String name) {
        if (MvConfig.HANDLER_DICTIONARY.equalsIgnoreCase(name)) {
            return stopDictionaryHandler();
        }
        MvJobController c = handlers.remove(name);
        if (c == null) {
            return false;
        }
        c.stop();
        return true;
    }

    @Override
    public synchronized boolean startScan(String handlerName, String targetName) {
        MvJobController c = handlers.get(handlerName);
        if (c == null) {
            throw new IllegalArgumentException("Unknown handler name: " + handlerName);
        }
        return c.startScan(targetName, getScanSettings());
    }

    /**
     * Stops the full scan for the specified target in the specified handler.
     * For illegal arguments, false is returned.
     *
     * @param handlerName Name of the handler
     * @param targetName Name of the target
     * @return true, if the scan was started, false otherwise
     */
    @Override
    public synchronized boolean stopScan(String handlerName, String targetName) {
        MvJobController c = handlers.get(handlerName);
        if (c == null) {
            return false;
        }
        return c.stopScan(targetName);
    }

    @Override
    public void printIssues(PrintStream pw) {
        new MvIssuePrinter(metadata).write(pw);
    }

    /**
     * Generate the set of SQL statements and print.
     */
    @Override
    public void printSql(PrintStream pw) {
        new MvSqlPrinter(metadata).write(pw);
    }

    /**
     * Start the default handlers.
     */
    @Override
    public void startDefaultHandlers() {
        if (LOG.isInfoEnabled()) {
            String msg = new MvIssuePrinter(metadata).write();
            LOG.info("\n"
                    + "---- BEGIN CONTEXT INFO ----\n"
                    + "{}\n"
                    + "----- END CONTEXT INFO -----", msg);
        }
        if (!metadata.isValid()) {
            throw new IllegalStateException(
                    "Refusing to start due to configuration errors.");
        }
        for (String handlerName : parseActiveHandlerNames()) {
            try {
                startHandler(handlerName);
            } catch (Exception ex) {
                LOG.error("Failed to activate the handler {}", handlerName, ex);
            }
        }
    }

    /**
     * Start and run the set of default handlers.
     */
    @Override
    public void runDefaultHandlers() {
        startDefaultHandlers();
        while (isRunning()) {
            YdbMisc.sleep(100L);
        }
    }

    private List<String> parseActiveHandlerNames() {
        String v = ydb.getConfig().getProperties().getProperty(MvConfig.CONF_HANDLERS);
        if (v == null) {
            return Collections.emptyList();
        }
        v = v.trim();
        if (v.length() == 0) {
            return Collections.emptyList();
        }
        return Arrays.asList(v.split("[,]"));
    }

    private synchronized ArrayList<MvJobController> grabControllers() {
        return new ArrayList<>(handlers.values());
    }

    private void refreshMetadata() {
        if (!metadata.isValid()) {
            LOG.warn("Parser produced errors, metadata retrieval skipped.");
        } else {
            LOG.info("Loading metadata and performing validation...");
            metadata.linkAndValidate(new MvDescriberYdb(ydb));
        }
    }

    private void sleepSome() {
        for (int i = 0; i < 3000; ++i) {
            if (!isRunning()) {
                break;
            }
            YdbMisc.sleep(100L);
        }
    }

    private void checkAndRunScheduledTasks() {
        for (MvJobController c : grabControllers()) {
            c.getApplyManager().refreshSelectors(ydb.getTableClient());
            c.getApplyManager().pingTasks();
        }
    }

    private class SchedulerTask implements Runnable {

        @Override
        public void run() {
            sleepSome();
            while (isRunning()) {
                checkAndRunScheduledTasks();
                sleepSome();
            }
        }
    }
}
