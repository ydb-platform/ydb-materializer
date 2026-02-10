package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * SQL expression used to grab the data to the materialized view.
 *
 * @author zinal
 */
public class MvViewExpr implements MvSqlPosHolder {

    public static final String ALIAS_DEFAULT = "default";

    // fields grabbed from the SQL statement
    private final MvView view;
    private final String alias;
    private final ArrayList<MvJoinSource> sources = new ArrayList<>();
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private final LinkedHashMap<String, MvLiteral> literals = new LinkedHashMap<>();
    private MvComputation filter;
    private final MvSqlPos sqlPos;
    // fields computed later or added based on the database metadata
    private MvUsedColumns usedColumns;

    public MvViewExpr(MvView view, String alias, MvSqlPos sqlPos) {
        this.view = view;
        this.alias = alias;
        this.sqlPos = sqlPos;
    }

    public MvViewExpr(MvView view, String alias) {
        this(view, alias, MvSqlPos.EMPTY);
    }

    public MvViewExpr(MvView view) {
        this(view, ALIAS_DEFAULT, MvSqlPos.EMPTY);
    }

    public MvViewExpr(String name) {
        this(new MvView(name, null, MvSqlPos.EMPTY));
        this.view.addPart(this);
    }

    /**
     * @return true, if the target uses non-literal computational output
     * columns, and false otherwise
     */
    public boolean hasComputationColumns() {
        for (MvColumn mc : columns) {
            if (mc.isComputation()
                    && !mc.getComputation().isLiteral()) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true, if the transformation is a single-step operation without
     * joins and complex computations, and false otherwise
     */
    public boolean isSingleStepTransformation() {
        return (sources.size() == 1)
                && (filter == null || filter.isEmpty())
                && !hasComputationColumns();
    }

    /**
     * @return true, if a single-step transformation is based on just the
     * primary key
     */
    public boolean isKeyOnlyTransformation() {
        if (!isSingleStepTransformation()) {
            return false;
        }
        if (sources.isEmpty()) {
            return true; // constant output - works for our case
        }
        List<String> key = sources.get(0).getTableInfo().getKey();
        for (MvColumn mc : columns) {
            if (!mc.isReference()) {
                continue;
            }
            if (!key.contains(mc.getSourceColumn())) {
                return false;
            }
        }
        return true;
    }

    public MvJoinSource getSourceByAlias(String name) {
        if (name == null) {
            return null;
        }
        for (MvJoinSource tr : sources) {
            if (name.equalsIgnoreCase(tr.getTableAlias())) {
                return tr;
            }
        }
        return null;
    }

    public MvJoinSource getTopMostSource() {
        if (sources.isEmpty()) {
            throw new IllegalStateException("No join sources defined in target "
                    + getName() + " as " + getAlias());
        }
        return sources.get(0);
    }

    public MvView getView() {
        return view;
    }

    public String getName() {
        return view.getName();
    }

    public String getAlias() {
        return alias;
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
        if (value == null) {
            throw new NullPointerException();
        }
        value = value.trim();
        MvLiteral l = literals.get(value);
        if (l == null) {
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
        return view.getTableInfo();
    }

    public void setTableInfo(MvTableInfo ti) {
        if (view.getTableInfo() == null) {
            view.setTableInfo(ti);
        }
        if (view.getTableInfo() != ti) {
            throw new IllegalArgumentException("Incompatible table info set");
        }
    }

    public void updateUsedColumns() {
        this.usedColumns = new MvUsedColumns();
        this.usedColumns.fill(this);
    }

    public MvUsedColumns getUsedColumns() {
        return usedColumns;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    @Override
    public String toString() {
        return "MV `" + getName() + "` AS " + getAlias();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 13 * hash + Objects.hashCode(this.getName());
        hash = 13 * hash + Objects.hashCode(alias);
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
        final MvViewExpr other = (MvViewExpr) obj;
        if (!Objects.equals(this.getName(), other.getName())) {
            return false;
        }
        return Objects.equals(this.alias, other.alias);
    }

}
