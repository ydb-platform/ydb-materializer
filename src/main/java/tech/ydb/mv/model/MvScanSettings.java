package tech.ydb.mv.model;

import java.io.Serializable;
import java.util.Properties;

import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvScanSettings implements Serializable {
    private static final long serialVersionUID = 202500908001L;

    private String controlTableName = MvConfig.DEF_SCAN_TABLE;
    private int rowsPerSecondLimit = 10000;

    public MvScanSettings() {
    }

    public MvScanSettings(MvScanSettings other) {
        this.controlTableName = other.controlTableName;
        this.rowsPerSecondLimit = other.rowsPerSecondLimit;
    }

    public MvScanSettings(Properties props) {
        this.controlTableName = props.getProperty(MvConfig.CONF_SCAN_TABLE, MvConfig.DEF_SCAN_TABLE);
        String v = props.getProperty(MvConfig.CONF_SCAN_RATE, "10000");
        this.rowsPerSecondLimit = Integer.parseInt(v);
    }

    public String getControlTableName() {
        return controlTableName;
    }

    public void setControlTableName(String controlTableName) {
        this.controlTableName = controlTableName;
    }

    public int getRowsPerSecondLimit() {
        return rowsPerSecondLimit;
    }

    public void setRowsPerSecondLimit(int rowsPerSecondLimit) {
        this.rowsPerSecondLimit = rowsPerSecondLimit;
    }

}
