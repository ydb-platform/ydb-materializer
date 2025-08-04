package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvComputation implements MvSqlPosHolder {

    private final ArrayList<Source> sources = new ArrayList<>();
    private String expression;
    private MvSqlPos sqlPos;

    public MvComputation(MvSqlPos sqlPos) {
        this.sqlPos = sqlPos;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public ArrayList<Source> getSources() {
        return sources;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

    public void setSqlPos(MvSqlPos inputPosition) {
        this.sqlPos = inputPosition;
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
