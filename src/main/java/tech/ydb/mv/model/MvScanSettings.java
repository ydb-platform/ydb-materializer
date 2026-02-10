package tech.ydb.mv.model;

import java.io.Serializable;
import java.util.Properties;

import tech.ydb.mv.MvConfig;

/**
 * Settings for full table scans (batch mode).
 *
 * @author zinal
 */
public class MvScanSettings implements Serializable {

    private static final long serialVersionUID = 202500926001L;

    private int rowsPerSecondLimit = 10000;

    /**
     * Create settings with default values.
     */
    public MvScanSettings() {
    }

    /**
     * Copy constructor.
     *
     * @param other Settings to copy.
     */
    public MvScanSettings(MvScanSettings other) {
        this.rowsPerSecondLimit = other.rowsPerSecondLimit;
    }

    /**
     * Create settings from properties.
     *
     * @param props Properties to read settings from.
     */
    public MvScanSettings(Properties props) {
        this.rowsPerSecondLimit = MvConfig.parseInt(props, MvConfig.CONF_SCAN_RATE, 10000);
    }

    /**
     * Get scan throttling limit.
     *
     * @return Rows-per-second limit used for scans.
     */
    public int getRowsPerSecondLimit() {
        return rowsPerSecondLimit;
    }

    /**
     * Set scan throttling limit.
     *
     * @param rowsPerSecondLimit Rows-per-second limit used for scans.
     */
    public void setRowsPerSecondLimit(int rowsPerSecondLimit) {
        this.rowsPerSecondLimit = rowsPerSecondLimit;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + this.rowsPerSecondLimit;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvScanSettings other = (MvScanSettings) obj;
        return (this.rowsPerSecondLimit == other.rowsPerSecondLimit);
    }

}
