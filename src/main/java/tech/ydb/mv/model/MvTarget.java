package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;

/**
 *
 * @author mzinal
 */
public class MvTarget implements MvSqlPosHolder {

    private final String name;
    private final ArrayList<MvJoinSource> sources = new ArrayList<>();
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private final LinkedHashMap<String, MvLiteral> literals = new LinkedHashMap<>();
    private MvComputation filter;
    private MvSqlPos sqlPos;

    public MvTarget(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
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

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    public void setSqlPos(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
    }

    @Override
    public String toString() {
        return "MV `" + name + "`";
    }

}
