package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvTarget implements MvPositionHolder {

    private String name;
    private final ArrayList<MvTableRef> sources = new ArrayList<>();
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private MvComputation filter;
    private MvInputPosition inputPosition;

    public MvTarget(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

    public MvTableRef getSourceByName(String name) {
        if (name == null) {
            return null;
        }
        for (MvTableRef tr : sources) {
            if (name.equalsIgnoreCase(tr.getAlias()))
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

    public ArrayList<MvTableRef> getSources() {
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
