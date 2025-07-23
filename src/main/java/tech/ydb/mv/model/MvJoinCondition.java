package tech.ydb.mv.model;

/**
 *
 * @author mzinal
 */
public class MvJoinCondition {

    private MvTableRef rightRef;
    private String rightAlias;
    private String rightColumn;
    private String leftLiteral;
    private MvTableRef leftRef;
    private String leftAlias;
    private String leftColumn;

    public MvJoinCondition(MvTableRef rightRef, String rightColumn) {
        this.rightRef = rightRef;
        this.rightAlias = rightRef.getAlias();
        this.rightColumn = rightColumn;
    }

    public boolean isReady() {
        return (leftLiteral!=null) || (leftRef!=null && leftColumn!=null);
    }

    public boolean isLiteral() {
        return leftLiteral != null;
    }

    public MvTableRef getRightRef() {
        return rightRef;
    }

    public void setRightRef(MvTableRef rightRef) {
        this.rightRef = rightRef;
    }

    public String getRightAlias() {
        return rightAlias;
    }

    public void setRightAlias(String rightAlias) {
        this.rightAlias = rightAlias;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    public void setRightColumn(String rightColumn) {
        this.rightColumn = rightColumn;
    }

    public String getLeftLiteral() {
        return leftLiteral;
    }

    public void setLeftLiteral(String leftLiteral) {
        this.leftLiteral = leftLiteral;
    }

    public MvTableRef getLeftRef() {
        return leftRef;
    }

    public void setLeftRef(MvTableRef leftRef) {
        this.leftRef = leftRef;
    }

    public String getLeftAlias() {
        return leftAlias;
    }

    public void setLeftAlias(String leftAlias) {
        this.leftAlias = leftAlias;
    }

    public String getLeftColumn() {
        return leftColumn;
    }

    public void setLeftColumn(String leftColumn) {
        this.leftColumn = leftColumn;
    }

}
