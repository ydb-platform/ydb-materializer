package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Objects;

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
