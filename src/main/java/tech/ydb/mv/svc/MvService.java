package tech.ydb.mv.svc;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.MvApi;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.metrics.MvMetrics;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.parser.MvDescriberYdb;
import tech.ydb.mv.parser.MvStreamBuilder;
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
    private final String identification;
    private final MvMetadata metadata;
    private final MvLocker locker;
    private final AtomicReference<MvHandlerSettings> handlerSettings;
    private final AtomicReference<MvDictionarySettings> dictionarySettings;
    private final AtomicReference<MvScanSettings> scanSettings;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<ScheduledFuture<?>> slowFuture = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> fastFuture = new AtomicReference<>();
    private volatile MvDictionaryLogger dictionaryManager = null;
    private final HashMap<String, MvJobController> handlers = new HashMap<>();

    public MvService(YdbConnector ydb, String identification) {
        this.ydb = ydb;
        this.identification = identification;
        this.metadata = loadMetadata(ydb, null);
        if (ydb.isManagementEnabled()) {
            this.locker = new MvLocker(ydb.getConnMgt(), "service://" + identification);
        } else {
            this.locker = null;
        }
        this.handlerSettings = new AtomicReference<>(new MvHandlerSettings());
        this.dictionarySettings = new AtomicReference<>(new MvDictionarySettings());
        this.scanSettings = new AtomicReference<>(new MvScanSettings());
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public MvService(YdbConnector ydb) {
        this(ydb, MvApi.generateId());
    }

    @Override
    public String getIdentification() {
        return identification;
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    @Override
    public YdbConnector getYdb() {
        return ydb;
    }

    @Override
    public MvMetadata getMetadata() {
        return metadata;
    }

    @Override
    public MvLocker getLocker() {
        if (locker == null) {
            throw new IllegalStateException("Management connection has not been configured");
        }
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
    public void shutdown() {
        cancelRegularJobs();
        List<MvJobController> currentJobs;
        synchronized (this) {
            currentJobs = new ArrayList<>(handlers.values());
        }
        currentJobs.forEach(h -> h.signalStop());
        currentJobs.forEach(h -> h.close());
        stopDictionaryHandler();
        synchronized (this) {
            handlers.clear();
        }
        if (locker != null) {
            locker.releaseAll();
        }
    }

    @Override
    public void close() {
        shutdown();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30L, TimeUnit.SECONDS)) {
                LOG.warn("Service scheduler did not shut down in time.");
            }
        } catch (InterruptedException ix) {
            Thread.currentThread().interrupt();
            LOG.warn("Interruption on closure");
        }
        if (locker != null) {
            locker.close();
        }
    }

    public synchronized boolean startDictionaryHandler() {
        if (dictionaryManager != null) {
            return false;
        }
        MvMetadata m = loadMetadata(ydb, null);
        appendDictHist(m);
        dictionaryManager = new MvDictionaryLogger(m, ydb, dictionarySettings.get());
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
     * @return true, if handler has been started, false otherwise
     */
    @Override
    public boolean startHandler(String name) {
        if (MvConfig.HANDLER_COORDINATOR.equalsIgnoreCase(name)) {
            LOG.warn("Ignored start request for coordinator job");
            return false;
        }
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
        MvJobController c = handlers.get(name);
        if (c != null) {
            if (c.isRunning()) {
                return false;
            }
            handlers.remove(name);
        }
        MvMetadata m = loadMetadata(ydb, name);
        appendDictHist(m);
        MvHandler handler = m.getHandlers().get(name);
        if (handler == null) {
            throw new IllegalArgumentException("Unknown handler name: " + name);
        }
        c = new MvJobController(this, m, handler, settings);
        handlers.put(name, c);
        scheduleRegularJobs();
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
    public void generateStreams(boolean create, PrintStream pw) {
        for (var handler : metadata.getHandlers().values()) {
            new MvStreamBuilder(ydb, metadata, handler, pw, create).apply();
        }
    }

    @Override
    public void printIssues(PrintStream pw) {
        new MvIssuePrinter(metadata).write(pw);
    }

    /**
     * Generate the set of SQL statements and print.
     */
    @Override
    public void printBasicSql(PrintStream pw) {
        new MvSqlPrinter(metadata, false).write(pw);
    }

    /**
     * Generate the set of SQL statements and print.
     */
    @Override
    public void printDebugSql(PrintStream pw) {
        new MvSqlPrinter(metadata, true).write(pw);
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

    private void scheduleRegularJobs() {
        if (fastFuture.get() == null) {
            var f = scheduler.scheduleWithFixedDelay(
                    this::fastRefresh,
                    5,
                    5,
                    TimeUnit.SECONDS
            );
            f = fastFuture.getAndSet(f);
            if (f != null) {
                f.cancel(false);
            }
        }
        if (slowFuture.get() == null) {
            var f = scheduler.scheduleWithFixedDelay(
                    this::slowRefresh,
                    60,
                    60,
                    TimeUnit.SECONDS
            );
            f = slowFuture.getAndSet(f);
            if (f != null) {
                f.cancel(false);
            }
        }
    }

    private void cancelRegularJobs() {
        var f = slowFuture.getAndSet(null);
        if (f != null) {
            f.cancel(true);
        }
        f = fastFuture.getAndSet(null);
        if (f != null) {
            f.cancel(true);
        }
    }

    private void slowRefresh() {
        for (MvJobController c : grabControllers()) {
            c.getApplyManager().refreshSelectors(ydb.getTableClient());
            if (!c.validateDatabaseLock()) {
                LOG.warn("Database lock has been lost by the running handler `{}`",
                        c.getName());
                // Database lock check actions have been removed from here.
                // Single-instance Controller is used instead to validate
                // and enforce the state of all jobs globally.
            }
        }
    }

    private void fastRefresh() {
        for (MvJobController c : grabControllers()) {
            MvMetrics.recordHandlerState(c.getName(), c.isRunning(), c.isLocked());
        }
    }

    private synchronized ArrayList<MvJobController> grabControllers() {
        return new ArrayList<>(handlers.values());
    }

    private void appendDictHist(MvMetadata m) {
        String historyTableName = ydb.getProperty(MvConfig.CONF_DICT_HIST_TABLE, MvConfig.DEF_DICT_HIST_TABLE);
        var tableInfo = new MvDescriberYdb(ydb).describeTable(historyTableName, null);
        m.getTables().put(tableInfo.getName(), tableInfo);
    }

    private static MvMetadata loadMetadata(YdbConnector ydb, String handlerName) {
        var m = MvConfigReader.read(ydb);
        if (handlerName != null) {
            MvHandler h = m.getHandlers().get(handlerName);
            if (h == null) {
                throw new IllegalArgumentException("Unknown handler name: " + handlerName);
            }
            m = m.subset(h);
        }
        if (!m.isValid()) {
            LOG.warn("Parser produced errors, metadata retrieval skipped.");
        } else {
            LOG.info("Loading metadata and performing validation...");
            m.linkAndValidate(new MvDescriberYdb(ydb));
        }
        return m;
    }
}
