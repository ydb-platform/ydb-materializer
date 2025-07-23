package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvColumn {

    private String name;
    private String sourceAlias;
    private String sourceColumn;
    private MvComputation computation;

    public MvColumn(String name, String sourceAlias, String sourceColumn) {
        this.name = name;
        this.sourceAlias = sourceAlias;
        this.sourceColumn = sourceColumn;
    }

    public MvColumn(String name, MvComputation computation) {
        this.name = name;
        this.computation = computation;
    }

    public boolean isComputation() {
        return (computation != null);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSourceAlias() {
        return sourceAlias;
    }

    public void setSourceAlias(String sourceAlias) {
        this.sourceAlias = sourceAlias;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public MvComputation getComputation() {
        return computation;
    }

    public void setComputation(MvComputation computation) {
        this.computation = computation;
    }

}
