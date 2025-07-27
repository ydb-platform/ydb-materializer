package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvComputation implements MvPositionHolder {

    private final ArrayList<Source> sources = new ArrayList<>();
    private String expression;
    private MvInputPosition inputPosition;

    public MvComputation(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
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
    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
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
