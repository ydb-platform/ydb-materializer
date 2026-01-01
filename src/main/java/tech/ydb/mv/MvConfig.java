package tech.ydb.mv;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Configuration setting names.
 *
 * @author zinal
 */
public class MvConfig {

    /**
     * Gson instance for basic conversions.
     */
    public static final Gson GSON = new GsonBuilder().create();

    /**
     * System-used prefix for job names / handler names.
     */
    public static final String SYS_PREFIX = "ydbmv";

    /**
     * Name for the dictionary change logging handler.
     */
    public static final String HANDLER_DICTIONARY = "ydbmv$dictionary";

    /**
     * Name for the global job coordinator handler.
     */
    public static final String HANDLER_COORDINATOR = "ydbmv$coordinator";

    /**
     * FILE read input statements from file TABLE read input statements from
     * database table
     */
    public static final String CONF_INPUT_MODE = "job.input.mode";

    /**
     * Path to file, in case FILE mode is chosen.
     */
    public static final String CONF_INPUT_FILE = "job.input.file";

    /**
     * Name of the table, in case TABLE mode is chosen.
     */
    public static final String CONF_INPUT_TABLE = "job.input.table";

    /**
     * Comma-separated list of handler names to be activated on RUN action.
     */
    public static final String CONF_HANDLERS = "job.handlers";

    /**
     * Scan rate limiter, rows per second.
     */
    public static final String CONF_SCAN_RATE = "job.scan.rate";

    /**
     * Path to scan feeder position table.
     */
    public static final String CONF_SCAN_TABLE = "job.scan.table";

    /**
     * Path to dictionary history table.
     */
    public static final String CONF_DICT_HIST_TABLE = "job.dict.hist.table";

    /**
     * Dictionary history consumer.
     */
    public static final String CONF_DICT_CONSUMER = "job.dict.consumer";

    /**
     * Handler setting: period between dictionary scans, seconds.
     */
    public static final String CONF_DICT_SCAN_SECONDS = "job.dict.scan.seconds";

    /**
     * Handler setting: query timeout, seconds.
     */
    public static final String CONF_QUERY_TIMEOUT = "job.query.seconds";

    /**
     * Path to coordination service node.
     */
    public static final String CONF_COORD_PATH = "job.coordination.path";

    /**
     * Lock timeout for job coordination in seconds.
     */
    public static final String CONF_COORD_TIMEOUT = "job.coordination.timeout";

    /**
     * Handler setting: partitioning strategy (default HASH, possible RANGE).
     */
    public static final String CONF_PARTITIONING = "job.apply.partitioning";

    /**
     * Handler setting: number of threads in the CDC reader pool.
     */
    public static final String CONF_CDC_THREADS = "job.cdc.threads";

    /**
     * Handler setting: number of threads in the apply pool.
     */
    public static final String CONF_APPLY_THREADS = "job.apply.threads";

    /**
     * Handler setting: max number of elements in the apply queue, per thread.
     */
    public static final String CONF_APPLY_QUEUE = "job.apply.queue";

    /**
     * Handler setting: number of rows to be selected for batch processing.
     */
    public static final String CONF_BATCH_SELECT = "job.batch.select";

    /**
     * Handler setting: number of rows to be applied in a batch.
     */
    public static final String CONF_BATCH_UPSERT = "job.batch.upsert";

    /**
     * Handler setting: max number of changes to be scanned in a single batch.
     */
    public static final String CONF_MAX_ROW_CHANGES = "job.max.row.changes";

    /**
     * Default input SQL file name.
     */
    public static final String DEF_STMT_FILE = "mv.sql";

    /**
     * Default input SQL table name.
     */
    public static final String DEF_STMT_TABLE = "mv/statements";

    /**
     * Scan position control table name.
     */
    public static final String DEF_SCAN_TABLE = "mv/scans_state";

    /**
     * Dictionary history table name.
     */
    public static final String DEF_DICT_HIST_TABLE = "mv/dict_hist";

    /**
     * Coordination node path.
     */
    public static final String DEF_COORD_PATH = "mv/coordination";

    public static Mode parseMode(String v) {
        if (v == null) {
            return null;
        }
        v = v.trim();
        for (var m : Mode.values()) {
            if (m.name().equalsIgnoreCase(v)) {
                return m;
            }
        }
        return null;
    }

    public static Input parseInput(String v) {
        if (v == null) {
            return null;
        }
        v = v.trim();
        for (var i : Input.values()) {
            if (i.name().equalsIgnoreCase(v)) {
                return i;
            }
        }
        return null;
    }

    public static PartitioningStrategy parsePartitioning(String v) {
        if (v == null) {
            return null;
        }
        v = v.trim();
        for (var s : PartitioningStrategy.values()) {
            if (s.name().equalsIgnoreCase(v)) {
                return s;
            }
        }
        return null;
    }

    public static enum Mode {
        CHECK,
        SQL,
        LOCAL,
        JOB
    }

    public static enum Input {
        FILE,
        TABLE
    }

    public static enum PartitioningStrategy {
        RANGE,
        HASH
    }

}
