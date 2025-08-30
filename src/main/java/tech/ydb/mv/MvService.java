package tech.ydb.mv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

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

    private final YdbConnector connector;
    private final MvContext context;
    private final AtomicReference<MvHandlerSettings> defaultSettings = new AtomicReference<>(new MvHandlerSettings());
    private final HashMap<String, MvController> handlers = new HashMap<>();
    private final RefreshTask refreshTask = new RefreshTask();
    private Thread refreshThread = null;

    public MvService(YdbConnector connector, Properties props) {
        this.connector = connector;
        this.context = MvConfigReader.readContext(this.connector, props);
        refreshMetadata();
    }

    public MvService(YdbConnector connector) {
        this(connector, connector.getConfig().getProperties());
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvContext getContext() {
        return context;
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
            c = new MvController(connector, handler, settings);
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
        // TODO
    }

    /**
     * Generate the set of SQL statements and print to stdout.
     */
    public void printSql() {
        // TODO
    }

    /**
     * Start and run the set of default handlers.
     */
    public void runHandlers() {
        // TODO
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
            path = connector.getDatabase() + "/" + tabname;
        }
        LOG.info("Describing table {} ...", path);
        TableDescription desc;
        try {
            DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            desc = connector.getTableRetryCtx()
                    .supplyResult(sess -> sess.describeTable(path, dts))
                    .join().getValue();
        } catch(Exception ex) {
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
            c.getApplyManager().refreshSelectors(connector.getTableClient());
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
