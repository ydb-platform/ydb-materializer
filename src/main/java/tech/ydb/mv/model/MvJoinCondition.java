package tech.ydb.mv.model;

/**
 * Join condition between sources in a view part (e.g. {@code a.id = b.id}).
 *
 * @author zinal
 */
public class MvJoinCondition implements MvSqlPosHolder {

    private MvLiteral firstLiteral;
    private MvJoinSource firstRef;
    private String firstAlias;
    private String firstColumn;
    private MvLiteral secondLiteral;
    private MvJoinSource secondRef;
    private String secondAlias;
    private String secondColumn;
    private final MvSqlPos sqlPos;

    /**
     * Create a join condition with a known SQL position.
     *
     * @param sqlPos Position in the SQL text.
     */
    public MvJoinCondition(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
    }

    /**
     * Create a join condition with an unknown/empty SQL position.
     */
    public MvJoinCondition() {
        this(MvSqlPos.EMPTY);
    }

    /**
     * Get left-hand literal.
     *
     * @return Literal on the left-hand side, if the left operand is a literal.
     */
    public MvLiteral getFirstLiteral() {
        return firstLiteral;
    }

    /**
     * Set left-hand literal.
     *
     * @param firstLiteral Literal on the left-hand side.
     */
    public void setFirstLiteral(MvLiteral firstLiteral) {
        this.firstLiteral = firstLiteral;
    }

    /**
     * Get left-hand source reference.
     *
     * @return Join source reference for the left-hand side, if the left operand
     * references a source.
     */
    public MvJoinSource getFirstRef() {
        return firstRef;
    }

    /**
     * Set left-hand source reference.
     *
     * @param firstRef Join source reference for the left-hand side.
     */
    public void setFirstRef(MvJoinSource firstRef) {
        this.firstRef = firstRef;
    }

    /**
     * Get left-hand source alias.
     *
     * @return Source alias for the left-hand side (may be {@code null}).
     */
    public String getFirstAlias() {
        return firstAlias;
    }

    /**
     * Set left-hand source alias.
     *
     * @param firstAlias Source alias for the left-hand side.
     */
    public void setFirstAlias(String firstAlias) {
        this.firstAlias = firstAlias;
    }

    /**
     * Get left-hand column name.
     *
     * @return Column name for the left-hand side (may be {@code null}).
     */
    public String getFirstColumn() {
        return firstColumn;
    }

    /**
     * Set left-hand column name.
     *
     * @param firstColumn Column name for the left-hand side.
     */
    public void setFirstColumn(String firstColumn) {
        this.firstColumn = firstColumn;
    }

    /**
     * Get right-hand literal.
     *
     * @return Literal on the right-hand side, if the right operand is a
     * literal.
     */
    public MvLiteral getSecondLiteral() {
        return secondLiteral;
    }

    /**
     * Set right-hand literal.
     *
     * @param secondLiteral Literal on the right-hand side.
     */
    public void setSecondLiteral(MvLiteral secondLiteral) {
        this.secondLiteral = secondLiteral;
    }

    /**
     * Get right-hand source reference.
     *
     * @return Join source reference for the right-hand side, if the right
     * operand references a source.
     */
    public MvJoinSource getSecondRef() {
        return secondRef;
    }

    /**
     * Set right-hand source reference.
     *
     * @param secondRef Join source reference for the right-hand side.
     */
    public void setSecondRef(MvJoinSource secondRef) {
        this.secondRef = secondRef;
    }

    /**
     * Get right-hand source alias.
     *
     * @return Source alias for the right-hand side (may be {@code null}).
     */
    public String getSecondAlias() {
        return secondAlias;
    }

    /**
     * Set right-hand source alias.
     *
     * @param secondAlias Source alias for the right-hand side.
     */
    public void setSecondAlias(String secondAlias) {
        this.secondAlias = secondAlias;
    }

    /**
     * Get right-hand column name.
     *
     * @return Column name for the right-hand side (may be {@code null}).
     */
    public String getSecondColumn() {
        return secondColumn;
    }

    /**
     * Set right-hand column name.
     *
     * @param secondColumn Column name for the right-hand side.
     */
    public void setSecondColumn(String secondColumn) {
        this.secondColumn = secondColumn;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    /**
     * Clone the current join condition to another instance of MV.
     *
     * @param part MV part to which the new condition will apply
     * @return The copy of the current condition object adopted to the specified
     * MV part instance.
     */
    public MvJoinCondition cloneTo(MvViewExpr part) {
        MvJoinCondition ret = new MvJoinCondition(sqlPos);
        if (firstLiteral != null) {
            ret.setFirstLiteral(part.addLiteral(firstLiteral.getValue()));
        } else {
            ret.setFirstAlias(firstAlias);
            ret.setFirstColumn(firstColumn);
            ret.setFirstRef(part.getSourceByAlias(firstAlias));
        }
        if (secondLiteral != null) {
            ret.setSecondLiteral(part.addLiteral(secondLiteral.getValue()));
        } else {
            ret.setSecondAlias(secondAlias);
            ret.setSecondColumn(secondColumn);
            ret.setSecondRef(part.getSourceByAlias(secondAlias));
        }
        return ret;
    }

}
