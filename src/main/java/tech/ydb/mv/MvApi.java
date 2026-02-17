package tech.ydb.mv;

import tech.ydb.mv.svc.MvService;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.svc.MvLocker;

/**
 * YDB Materializer controlling API.
 *
 * @author zinal
 */
public interface MvApi extends AutoCloseable {

    /**
     * Create the new service API instance.
     *
     * @param ydb YDB connection
     * @param identification Identification string
     * @return New service API instance created and configured
     */
    static MvApi newInstance(YdbConnector ydb, String identification) {
        return new MvService(ydb, identification);
    }

    /**
     * Create the new service API instance.
     *
     * @param ydb YDB connection
     * @return New service API instance created and configured
     */
    static MvApi newInstance(YdbConnector ydb) {
        return new MvService(ydb, generateId());
    }

    /**
     * Generate a unique runner ID.
     */
    static String generateId() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().encodeToString(bb.array()).substring(0, 22);
    }

    /**
     * @return Identification value
     */
    String getIdentification();

    /**
     * @return The instance of the scheduler to run the tasks against.
     */
    ScheduledExecutorService getScheduler();

    /**
     * @return YDB connector being used
     */
    YdbConnector getYdb();

    /**
     * @return The locker service enables the distributed concurrency control
     */
    MvLocker getLocker();

    /**
     * @return The complete metadata object being used
     */
    MvMetadata getMetadata();

    /**
     * Apply default settings being read from properties.
     *
     * @param props Input properties object
     */
    void applyDefaults(Properties props);

    /**
     * @return The copy of current default handler settings
     */
    MvHandlerSettings getHandlerSettings();

    /**
     * Set the new defaults for handler settings.
     *
     * @param defaultSettings The default settings to be used
     */
    void setHandlerSettings(MvHandlerSettings defaultSettings);

    /**
     * @return The copy of current dictionary processing settings.
     */
    MvDictionarySettings getDictionarySettings();

    /**
     * Set the new defaults for dictionary processing settings.
     *
     * @param defaultSettings The default settings to be used
     */
    void setDictionarySettings(MvDictionarySettings defaultSettings);

    /**
     * @return The copy of the current scan settings.
     */
    MvScanSettings getScanSettings();

    /**
     * Set the new defaults for scan settings.
     *
     * @param defaultSettings The default settings to be used
     */
    void setScanSettings(MvScanSettings defaultSettings);

    /**
     * @return true, if at least one handler is active, and false otherwise.
     */
    boolean isRunning();

    /**
     * Stop all the handlers which are running.
     */
    void shutdown();

    /**
     * Start the specified handler.
     *
     * @param handlerName The handler to be started
     * @return true, if the handler was started, false if it was already running
     */
    boolean startHandler(String handlerName);

    /**
     * Stop the specified handler.
     *
     * @param handlerName The handler to be stopped
     * @return true, if the handler was stopped, and false, if it was not
     * actually started
     */
    boolean stopHandler(String handlerName);

    /**
     * Start the full scan for the specified target in the specified handler.
     * For illegal arguments, exceptions are thrown.
     *
     * @param handlerName Name of the handler
     * @param targetName Name of the target
     * @return true, if the handler was started, false if it was already running
     */
    boolean startScan(String handlerName, String targetName);

    /**
     * Stop the full scan for the specified target in the specified handler.
     *
     * @param handlerName Name of the handler
     * @param targetName Name of the target
     * @return true, if the scan was stopped, and false, if it was not actually
     * started
     */
    boolean stopScan(String handlerName, String targetName);

    /**
     * Generate the set of SQL statements for CDC streams, print and optionally
     * apply to the database.
     *
     * @param create true, if the missing CDC streams and consumers should be
     * created, and false otherwise.
     * @param pw The output print stream
     */
    void generateStreams(boolean create, PrintStream pw);

    /**
     * Print the list of issues in the current context
     *
     * @param pw The output print stream
     */
    void printIssues(PrintStream pw);

    /**
     * Generate the set of basic SQL statements used and print.
     *
     * @param pw The output print stream
     */
    void printBasicSql(PrintStream pw);

    /**
     * Generate the set of internal SQL statements used internally and print.
     *
     * @param pw The output print stream
     */
    void printDebugSql(PrintStream pw);

    /**
     * Start the default handlers (as listed in the properties) and return.
     */
    void startDefaultHandlers();

    /**
     * Start the default handlers (as listed in the properties) and continue to
     * run until the termination is requested via shutdown().
     */
    void runDefaultHandlers();

}
