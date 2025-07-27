package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvTarget {

    private String name;
    private final ArrayList<MvTableRef> sources = new ArrayList<>();
    private final ArrayList<MvColumn> columns = new ArrayList<>();
    private MvComputation filter;
    private MvInputPosition inputPosition;

    public MvTarget(String name) {
        this.name = name;
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

    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

}
