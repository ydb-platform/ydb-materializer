package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Materialized view defined as a target of the transformation.
 * @author zinal
 */
public class MvTarget implements MvSqlPosHolder {

    private final String name;
    private final ArrayList<MvJoinSource> sources = new ArrayList<>();
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private final LinkedHashMap<String, MvLiteral> literals = new LinkedHashMap<>();
    private MvComputation filter;
    private MvTableInfo tableInfo;
    private final MvSqlPos sqlPos;

    public MvTarget(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
    }

    public MvTarget(String name) {
        this(name, MvSqlPos.EMPTY);
    }

    /**
     * @return true, if the target uses non-literal computational output columns, and false otherwise
     */
    public boolean hasComputationColumns() {
        for (MvColumn mc : columns) {
            if (mc.isComputation() &&
                    ! mc.getComputation().isLiteral()) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true, if the transformation is a single-step operation
     * without joins and complex computations, and false otherwise
     */
    public boolean isSingleStepTransformation() {
        return (sources.size() == 1)
                && (filter==null || filter.isEmpty())
                && !hasComputationColumns();
    }

    /**
     * @return true, if a single-step transformation is based on just the primary key
     */
    public boolean isKeyOnlyTransformation() {
        if (! isSingleStepTransformation()) {
            return false;
        }
        if (sources.isEmpty()) {
            return true; // constant output - works for our case
        }
        List<String> key = sources.get(0).getTableInfo().getKey();
        for (MvColumn mc : columns) {
            if (! mc.isReference()) {
                continue;
            }
            if (! key.contains(mc.getSourceColumn())) {
                return false;
            }
        }
        return true;
    }

    public MvJoinSource getSourceByAlias(String name) {
        if (name==null) {
            return null;
        }
        for (MvJoinSource tr : sources) {
            if (name.equalsIgnoreCase(tr.getTableAlias()))
                return tr;
        }
        return null;
    }

    public List<String> getInputKeyColumns() {
        if (sources.isEmpty() || sources.get(0).getTableInfo()==null) {
            throw new IllegalStateException();
        }
        return sources.get(0).getTableInfo().getKey();
    }

    public String getName() {
        return name;
    }

    public ArrayList<MvJoinSource> getSources() {
        return sources;
    }

    public ArrayList<MvColumn> getColumns() {
        return columns;
    }

    public MvComputation getFilter() {
        return filter;
    }

    public void setFilter(MvComputation filter) {
        this.filter = filter;
    }

    public MvLiteral addLiteral(String value) {
        if (value==null) {
            throw new NullPointerException();
        }
        value = value.trim();
        MvLiteral l = literals.get(value);
        if (l==null) {
            l = new MvLiteral(value, literals.size());
            literals.put(value, l);
        }
        return l;
    }

    public MvLiteral getLiteral(String value) {
        return literals.get(value);
    }

    public Collection<MvLiteral> getLiterals() {
        return literals.values();
    }

    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(MvTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    @Override
    public String toString() {
        return "MV `" + name + "`";
    }

}
