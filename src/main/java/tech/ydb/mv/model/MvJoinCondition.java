package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvJoinCondition {

    private String firstLiteral;
    private MvTableRef firstRef;
    private String firstAlias;
    private String firstColumn;
    private String secondLiteral;
    private MvTableRef secondRef;
    private String secondAlias;
    private String secondColumn;
    private MvInputPosition inputPosition;

    public MvJoinCondition() {
    }

    public String getFirstLiteral() {
        return firstLiteral;
    }

    public void setFirstLiteral(String firstLiteral) {
        this.firstLiteral = firstLiteral;
    }

    public MvTableRef getFirstRef() {
        return firstRef;
    }

    public void setFirstRef(MvTableRef firstRef) {
        this.firstRef = firstRef;
    }

    public String getFirstAlias() {
        return firstAlias;
    }

    public void setFirstAlias(String firstAlias) {
        this.firstAlias = firstAlias;
    }

    public String getFirstColumn() {
        return firstColumn;
    }

    public void setFirstColumn(String firstColumn) {
        this.firstColumn = firstColumn;
    }

    public String getSecondLiteral() {
        return secondLiteral;
    }

    public void setSecondLiteral(String secondLiteral) {
        this.secondLiteral = secondLiteral;
    }

    public MvTableRef getSecondRef() {
        return secondRef;
    }

    public void setSecondRef(MvTableRef secondRef) {
        this.secondRef = secondRef;
    }

    public String getSecondAlias() {
        return secondAlias;
    }

    public void setSecondAlias(String secondAlias) {
        this.secondAlias = secondAlias;
    }

    public String getSecondColumn() {
        return secondColumn;
    }

    public void setSecondColumn(String secondColumn) {
        this.secondColumn = secondColumn;
    }

    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

}
