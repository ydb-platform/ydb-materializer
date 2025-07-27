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
    private MvInputPosition inputPosition;

    public MvColumn(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
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

    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

}
