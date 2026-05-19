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
    private final String destinationName;
    private final MvSqlPos sqlPos;
    private final HashMap<String, MvViewExpr> parts = new HashMap<>();
    private final HashMap<MvViewOption, Object> options = new HashMap<>();
    // fields computed later or added based on the database metadata
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private MvTableInfo tableInfo;

    public MvView(String viewName, String destinationName, MvSqlPos sqlPos) {
        this.viewName = viewName;
        this.destinationName = destinationName;
        this.sqlPos = sqlPos;
        // initializing default values for all view options
        for (var vo : MvViewOption.ENTRIES.values()) {
            this.options.put(vo, vo.defaultValue());
        }
    }

    public String getName() {
        return viewName;
    }

    public String getDestination() {
        return destinationName;
    }

    public boolean isDefaultDestination() {
        if (destinationName == null || destinationName.length() == 0) {
            return true;
        }
        return false;
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

    public HashMap<String, MvViewExpr> getParts() {
        return parts;
    }

    public HashMap<MvViewOption, Object> getOptions() {
        return options;
    }

    public boolean isSkipDeletes() {
        return Boolean.TRUE.equals(options.get(MvViewOption.SKIP_DELETES));
    }

    public ArrayList<MvColumn> getColumns() {
        return columns;
    }

    public MvViewExpr addPart(MvViewExpr t) {
        return parts.put(t.getAlias(), t);
    }

    public void updateUsedColumns() {
        for (MvViewExpr t : parts.values()) {
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

    public String[] getKeyColumnNames() {
        return tableInfo.getKey().toArray(String[]::new);
    }

    @Override
    public String toString() {
        if (isSkipDeletes()) {
            return "MV `" + viewName + "` (SKIP_DELETES)";
        }
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
