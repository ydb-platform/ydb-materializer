package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author zinal
 */
public class MvComputation implements MvSqlPosHolder {

    private final String expression;
    private final MvLiteral literal;
    private final ArrayList<Source> sources = new ArrayList<>();
    private final MvSqlPos sqlPos;

    public MvComputation(String expression, MvSqlPos sqlPos) {
        this.expression = expression;
        this.literal = null;
        this.sqlPos = sqlPos;
    }

    public MvComputation(MvLiteral literal, MvSqlPos sqlPos) {
        this.expression = null;
        this.literal = literal;
        this.sqlPos = sqlPos;
    }

    public MvComputation(String expression) {
        this(expression, MvSqlPos.EMPTY);
    }

    public MvComputation(MvLiteral literal) {
        this(literal, MvSqlPos.EMPTY);
    }

    public boolean isEmpty() {
        return (literal==null) &&
                ((expression==null) || (expression.trim().length()==0));
    }

    public boolean isLiteral() {
        return (literal!=null);
    }

    public String getExpression() {
        return expression;
    }

    public MvLiteral getLiteral() {
        return literal;
    }

    public ArrayList<Source> getSources() {
        return sources;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    public static class Source {

        private String alias;
        private MvJoinSource reference;

        public Source(String alias) {
            this.alias = alias;
        }

        public Source(String alias, MvJoinSource reference) {
            this.alias = alias;
            this.reference = reference;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public MvJoinSource getReference() {
            return reference;
        }

        public void setReference(MvJoinSource reference) {
            this.reference = reference;
        }

    }

}
