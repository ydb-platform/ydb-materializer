package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvComputation {

    private final ArrayList<Source> sources = new ArrayList<>();
    private String expression;

    public MvComputation(String expression) {
        this.expression = expression;
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

    public static class Source {

        private String alias;
        private MvTableRef reference;

        public Source(String alias) {
            this.alias = alias;
        }

        public Source(String alias, MvTableRef reference) {
            this.alias = alias;
            this.reference = reference;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public MvTableRef getReference() {
            return reference;
        }

        public void setReference(MvTableRef reference) {
            this.reference = reference;
        }

    }

}
