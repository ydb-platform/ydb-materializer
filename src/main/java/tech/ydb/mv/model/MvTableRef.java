package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvTableRef {

    private String reference;
    private String alias;
    private Mode mode;
    private final ArrayList<MvJoinCondition> conditions = new ArrayList<>();

    public MvTableRef(String reference, String alias, Mode mode) {
        this.reference = reference;
        this.alias = alias;
        this.mode = mode;
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

    public static enum Mode {
        MAIN,
        INNER,
        LEFT
    }
}
