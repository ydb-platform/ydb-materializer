package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvTableRef implements MvPositionHolder {

    private String reference;
    private String alias;
    private Mode mode;
    private final ArrayList<MvJoinCondition> conditions = new ArrayList<>();
    private MvInputPosition inputPosition;

    public MvTableRef(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public ArrayList<MvJoinCondition> getConditions() {
        return conditions;
    }

    @Override
    public MvInputPosition getInputPosition() {
        return inputPosition;
    }

    public void setInputPosition(MvInputPosition inputPosition) {
        this.inputPosition = inputPosition;
    }

    public static enum Mode {
        MAIN,
        INNER,
        LEFT
    }
}
