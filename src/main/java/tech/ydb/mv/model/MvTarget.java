package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/**
 *
 * @author mzinal
 */
public class MvTarget implements MvPositionHolder {

    private String name;
    private final ArrayList<MvJoinSource> sources = new ArrayList<>();
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private final LinkedHashMap<String, MvLiteral> literals = new LinkedHashMap<>();
    private MvComputation filter;
    private MvInputPosition inputPosition;

    public MvTarget(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
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

    public void setName(String name) {
        this.name = name;
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
    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

    @Override
    public String toString() {
        return "MV `" + name + "`";
    }

}
