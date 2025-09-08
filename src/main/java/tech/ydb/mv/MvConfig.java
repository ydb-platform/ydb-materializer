package tech.ydb.mv;

import java.util.Properties;

import tech.ydb.mv.model.MvHandlerSettings;

/**
 * Configuration setting names.
 *
 * @author zinal
 */
public class MvConfig {

    public static final String DICTINARY_HANDLER = "dictionary";

    /**
     * FILE        read input statements from file
     * TABLE       read input statements from database table
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
     * Path to scan feeder position table.
     */
    public static final String CONF_SCAN_TABLE = "job.scan.table";

    /**
     * Path to dictionary history table.
     */
    public static final String CONF_DICT_TABLE = "job.dict.table";

    /**
     * Dictionary history consumer.
     */
    public static final String CONF_DICT_CONSUMER = "job.dict.consumer";

    /**
     * Scan rate limiter, rows per second.
     */
    public static final String CONF_SCAN_RATE = "job.scan.rate";

    /**
     * Path to coordination service node.
     */
    public static final String CONF_COORD_PATH = "job.coordination.path";

    /**
     * Handler setting: number of threads in the CDC reader pool.
     */
    public static final String CONF_DEF_CDC_THREADS = "job.default.cdc.threads";

    /**
     * Handler setting: number of threads in the apply pool.
     */
    public static final String CONF_DEF_APPLY_THREADS = "job.default.apply.threads";

    /**
     * Handler setting: max number of elements in the apply queue, per thread.
     */
    public static final String CONF_DEF_APPLY_QUEUE = "job.default.apply.queue";

    /**
     * Handler setting: number of rows to be selected for batch processing.
     */
    public static final String CONF_DEF_BATCH_SELECT = "job.default.batch.select";

    /**
     * Handler setting: number of rows to be applied in a batch.
     */
    public static final String CONF_DEF_BATCH_UPSERT = "job.default.batch.upsert";


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
    public static final String DEF_DICT_TABLE = "mv/dict_hist";

    /**
     * Coordination node path.
     */
    public static final String DEF_COORD_PATH = "mv/coordination";


    public static Mode parseMode(String v) {
        if (v==null) {
            return null;
        }
        v = v.trim();
        for (Mode m : Mode.values()) {
            if (m.name().equalsIgnoreCase(v)) {
                return m;
            }
        }
        return null;
    }

    public static Input parseInput(String v) {
        if (v==null) {
            return null;
        }
        v = v.trim();
        for (Input i : Input.values()) {
            if (i.name().equalsIgnoreCase(v)) {
                return i;
            }
        }
        return null;
    }

    public static enum Mode {
        CHECK,
        SQL,
        RUN
    }

    public static enum Input {
        FILE,
        TABLE
    }

}
