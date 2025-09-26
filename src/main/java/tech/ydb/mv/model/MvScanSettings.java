package tech.ydb.mv.model;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvScanSettings implements Serializable {
    private static final long serialVersionUID = 202500926001L;

    private int rowsPerSecondLimit = 10000;

    public MvScanSettings() {
    }

    public MvScanSettings(MvScanSettings other) {
        this.rowsPerSecondLimit = other.rowsPerSecondLimit;
    }

    public MvScanSettings(Properties props) {
        String v = props.getProperty(MvConfig.CONF_SCAN_RATE, "10000");
        this.rowsPerSecondLimit = Integer.parseInt(v);
    }

    public int getRowsPerSecondLimit() {
        return rowsPerSecondLimit;
    }

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
