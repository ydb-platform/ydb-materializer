package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Computed value used for a view column or filter: an expression or a literal.
 *
 * @author zinal
 */
public class MvComputation implements MvSqlPosHolder {

    private final String expression;
    private final MvLiteral literal;
    private final ArrayList<Source> sources = new ArrayList<>();
    private final MvSqlPos sqlPos;

    /**
     * Create a computation based on an SQL expression.
     *
     * @param expression Expression text.
     * @param sqlPos Position in the SQL text.
     */
    public MvComputation(String expression, MvSqlPos sqlPos) {
        this.expression = expression;
        this.literal = null;
        this.sqlPos = sqlPos;
    }

    /**
     * Create a computation based on a literal.
     *
     * @param literal Literal value.
     * @param sqlPos Position in the SQL text.
     */
    public MvComputation(MvLiteral literal, MvSqlPos sqlPos) {
        this.expression = null;
        this.literal = literal;
        this.sqlPos = sqlPos;
    }

    /**
     * Create a computation based on an SQL expression with an empty SQL
     * position.
     *
     * @param expression Expression text.
     */
    public MvComputation(String expression) {
        this(expression, MvSqlPos.EMPTY);
    }

    /**
     * Create a computation based on a literal with an empty SQL position.
     *
     * @param literal Literal value.
     */
    public MvComputation(MvLiteral literal) {
        this(literal, MvSqlPos.EMPTY);
    }

    /**
     * Check whether computation is empty.
     *
     * @return {@code true} if both expression and literal are empty/missing.
     */
    public boolean isEmpty() {
        return (literal == null)
                && ((expression == null) || (expression.trim().length() == 0));
    }

    /**
     * Check whether computation is a literal.
     *
     * @return {@code true} if this computation is represented as a literal.
     */
    public boolean isLiteral() {
        return (literal != null);
    }

    /**
     * Get expression text.
     *
     * @return Expression text, or {@code null} if this computation is a
     * literal.
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Get literal value.
     *
     * @return Literal value, or {@code null} if this computation is an
     * expression.
     */
    public MvLiteral getLiteral() {
        return literal;
    }

    /**
     * Get sources referenced by this computation.
     *
     * @return Mutable list of sources referenced by the computation.
     */
    public ArrayList<Source> getSources() {
        return sources;
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 61 * hash + Objects.hashCode(this.expression);
        hash = 61 * hash + Objects.hashCode(this.literal);
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
        final MvComputation other = (MvComputation) obj;
        if (!Objects.equals(this.expression, other.expression)) {
            return false;
        }
        return Objects.equals(this.literal, other.literal);
    }

    public static class Source {

        private final String alias;
        private final String column;
        private MvJoinSource reference;

        /**
         * Create a source reference by alias and column.
         *
         * @param alias Source alias.
         * @param column Column name.
         */
        public Source(String alias, String column) {
            this.alias = alias;
            this.column = column;
        }

        /**
         * Create a source reference with a resolved join source.
         *
         * @param alias Source alias.
         * @param column Column name.
         * @param reference Resolved join source.
         */
        public Source(String alias, String column, MvJoinSource reference) {
            this.alias = alias;
            this.column = column;
            this.reference = reference;
        }

        /**
         * Get source alias.
         *
         * @return Source alias.
         */
        public String getAlias() {
            return alias;
        }

        /**
         * Get column name.
         *
         * @return Column name.
         */
        public String getColumn() {
            return column;
        }

        /**
         * Get resolved join source.
         *
         * @return Resolved join source reference (may be {@code null}).
         */
        public MvJoinSource getReference() {
            return reference;
        }

        /**
         * Set resolved join source.
         *
         * @param reference Resolved join source reference.
         */
        public void setReference(MvJoinSource reference) {
            this.reference = reference;
        }

    }

}
