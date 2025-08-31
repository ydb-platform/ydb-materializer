package tech.ydb.mv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

import tech.ydb.mv.format.MvIssuePrinter;
import tech.ydb.mv.format.MvSqlPrinter;
import tech.ydb.mv.parser.MvConfigReader;
import tech.ydb.mv.parser.MvValidator;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbMisc;

/**
 * Work context for YDB Materializer activities.
 *
 * @author zinal
 */
public class MvService {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvService.class);

    private final YdbConnector ydb;
    private final MvContext context;
    private final MvCoordinator coordinator;
    private final AtomicReference<MvHandlerSettings> defaultSettings;
    private final HashMap<String, MvController> handlers = new HashMap<>();
    private final RefreshTask refreshTask = new RefreshTask();
    private Thread refreshThread = null;

    public MvService(YdbConnector ydb) {
        this.ydb = ydb;
        this.context = MvConfigReader.readContext(ydb, ydb.getConfig().getProperties());
        this.coordinator = new MvCoordinator(ydb);
        this.defaultSettings = new AtomicReference<>(new MvHandlerSettings());
        refreshMetadata();
    }

    public YdbConnector getYdb() {
        return ydb;
    }

    public MvContext getContext() {
        return context;
    }

    public MvCoordinator getCoordinator() {
        return coordinator;
    }

    public MvHandlerSettings getDefaultSettings() {
        return new MvHandlerSettings(defaultSettings.get());
    }

    public void setDefaultSettings(MvHandlerSettings defaultSettings) {
        if (defaultSettings==null) {
            defaultSettings = new MvHandlerSettings();
        } else {
            defaultSettings = new MvHandlerSettings(defaultSettings);
        }
        this.defaultSettings.set(defaultSettings);
    }

    /**
     * @return true, if at least one handler is active, and false otherwise.
     */
    public synchronized boolean isRunning() {
        return !handlers.isEmpty();
    }

    /**
     * Stop all the handlers which are running.
     */
    public synchronized void shutdown() {
        handlers.values().forEach(h -> h.stop());
        handlers.clear();
        coordinator.releaseAll();
    }

    /**
     * Start the handler with the specified name, using the current default settings.
     * @param name Name of the handler to be started
     */
    public void startHandler(String name) {
        startHandler(name, getDefaultSettings());
    }

    /**
     * Start the handler with the specified settings.
     *
     * @param name Name of the handler to be started
     * @param settings The settings to be used by the handler
     */
    public synchronized void startHandler(String name, MvHandlerSettings settings) {
        MvController c = handlers.get(name);
        if (c==null) {
            MvHandler handler = context.getHandlers().get(name);
            if (handler==null) {
                throw new IllegalArgumentException("Unknown handler name: " + name);
            }
            c = new MvController(this, handler, settings);
            handlers.put(name, c);
        }
        c.start();
        if (refreshThread==null || !refreshThread.isAlive()) {
            refreshThread = new Thread(refreshTask);
            refreshThread.setName("MvServiceRefreshThread");
            refreshThread.setDaemon(true);
            refreshThread.start();
        }
    }

    /**
     * Stop the handler with the specified name.
     * @param name The name of the handler to be stopped
     * @return true, if the handler was actually stopped, and false otherwise
     */
    public synchronized boolean stopHandler(String name) {
        MvController c = handlers.remove(name);
        if (c == null) {
            return false;
        }
        c.stop();
        return true;
    }

    /**
     * Print the list of issues in the current context to stdout.
     */
    public void printIssues() {
        new MvIssuePrinter(context).write(System.out);
    }

    /**
     * Generate the set of SQL statements and print to stdout.
     */
    public void printSql() {
        new MvSqlPrinter(context).write(System.out);
    }

    /**
     * Start and run the set of default handlers.
     */
    public void runHandlers() {
        if (LOG.isInfoEnabled()) {
            String msg = new MvIssuePrinter(context).write();
            LOG.info("---- BEGIN CONTEXT INFO ----\n"
                    + "{}\n"
                    + "----- END CONTEXT INFO -----", msg);
        }
        parseHandlerSettings();
        for (String handlerName : parseActiveHandlerNames()) {
            try {
                startHandler(handlerName);
            } catch(Exception ex) {
                LOG.error("Failed to activate the handler {}", handlerName, ex);
            }
        }
        while (isRunning()) {
            YdbMisc.sleep(100L);
        }
    }

    private void parseHandlerSettings() {
        MvHandlerSettings settings  = new MvHandlerSettings();
        Properties props = ydb.getConfig().getProperties();
        String v;

        v = props.getProperty(App.CONF_DEF_CDC_THREADS, String.valueOf(settings.getCdcReaderThreads()));
        settings.setCdcReaderThreads(Integer.parseInt(v));

        v = props.getProperty(App.CONF_DEF_APPLY_THREADS, String.valueOf(settings.getApplyThreads()));
        settings.setApplyThreads(Integer.parseInt(v));

        v = props.getProperty(App.CONF_DEF_APPLY_QUEUE, String.valueOf(settings.getApplyQueueSize()));
        settings.setApplyQueueSize(Integer.parseInt(v));

        v = props.getProperty(App.CONF_DEF_BATCH_SELECT, String.valueOf(settings.getSelectBatchSize()));
        settings.setSelectBatchSize(Integer.parseInt(v));

        v = props.getProperty(App.CONF_DEF_BATCH_UPSERT, String.valueOf(settings.getUpsertBatchSize()));
        settings.setUpsertBatchSize(Integer.parseInt(v));

        setDefaultSettings(settings);
    }

    private List<String> parseActiveHandlerNames() {
        String v = ydb.getConfig().getProperties().getProperty(App.CONF_HANDLERS);
        if (v==null) {
            return Collections.emptyList();
        }
        v = v.trim();
        if (v.length()==0) {
            return Collections.emptyList();
        }
        return Arrays.asList(v.split("[,]"));
    }

    private synchronized ArrayList<MvController> grabControllers() {
        return new ArrayList<>(handlers.values());
    }

    private void refreshMetadata() {
        if (! context.isValid()) {
            LOG.warn("Context is not valid after parsing - metadata retrieval skipped.");
            return;
        }
        HashMap<String, MvTableInfo> info = new HashMap<>();
        for (String tabname : context.collectTables()) {
            MvTableInfo ti = describeTable(tabname);
            if (ti!=null) {
                info.put(tabname, ti);
            }
        }
        linkTables(info);
        validate();
    }

    private void linkTables(HashMap<String, MvTableInfo> info) {
        for (MvTarget t : context.getTargets().values()) {
            t.setTableInfo(info.get(t.getName()));
            for (MvJoinSource r : t.getSources()) {
                r.setTableInfo(info.get(r.getTableName()));
            }
        }
        for (MvHandler h : context.getHandlers().values()) {
            for (MvInput i : h.getInputs().values()) {
                i.setTableInfo(info.get(i.getTableName()));
            }
        }
    }

    private MvTableInfo describeTable(String tabname) {
        String path;
        if (tabname.startsWith("/")) {
            path = tabname;
        } else {
            path = ydb.getDatabase() + "/" + tabname;
        }
        LOG.info("Describing table {} ...", path);
        TableDescription desc;
        try {
            DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            desc = ydb.getTableRetryCtx()
                    .supplyResult(sess -> sess.describeTable(path, dts))
                    .join().getValue();
        } catch(Exception ex) {
            if (ex instanceof UnexpectedResultException) {
                Status status = ((UnexpectedResultException)ex).getStatus();
                if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                    LOG.warn("Failed to obtain description for table {} - table is missing or no access", path);
                    return null;
                }
            }
            LOG.warn("Failed to obtain description for table {}", path, ex);
            return null;
        }

        return new MvTableInfo(tabname, path, desc);
    }

    private boolean validate() {
        if (! context.isValid()) {
            LOG.warn("Context already invalid, validation skipped.");
            return false;
        }
        return new MvValidator(context).validate();
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
        for (MvController c : grabControllers()) {
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
