package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 * Materialized view defined as a target of the transformation.
 *
 * @author zinal
 */
public class MvView implements MvSqlPosHolder {

    // fields grabbed from the SQL statement
    private final String viewName;
    private final MvSqlPos sqlPos;
    private final HashMap<String, MvTarget> targets = new HashMap<>();
    // fields computed later or added based on the database metadata
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private MvTableInfo tableInfo;

    public MvView(String viewName, MvSqlPos sqlPos) {
        this.viewName = viewName;
        this.sqlPos = sqlPos;
    }

    public String getName() {
        return viewName;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(MvTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public HashMap<String, MvTarget> getTargets() {
        return targets;
    }

    public ArrayList<MvColumn> getColumns() {
        return columns;
    }

    public MvTarget addTarget(MvTarget t) {
        return targets.put(t.getAlias(), t);
    }

    public void updateUsedColumns() {
        for (MvTarget t : targets.values()) {
            t.updateUsedColumns();
        }
    }

    public void addColumnIf(MvColumn column) {
        boolean found = false;
        for (MvColumn mc : columns) {
            if (mc.getName().equals(column.getName())) {
                found = true;
                break;
            }
        }
        if (!found) {
            columns.add(column);
        }
    }

    @Override
    public String toString() {
        return "MV `" + viewName + "`";
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 13 * hash + Objects.hashCode(this.viewName);
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
        final MvView other = (MvView) obj;
        return Objects.equals(this.viewName, other.viewName);
    }

}
