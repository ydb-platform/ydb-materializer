package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvContext {

    private final ArrayList<MvTarget> views = new ArrayList<>();
    private final ArrayList<MvInput> inputs = new ArrayList<>();
    private final ArrayList<MvIssue> errors = new ArrayList<>();
    private final ArrayList<MvIssue> warnings = new ArrayList<>();

    public boolean isValid() {
        return errors.isEmpty();
    }

    public ArrayList<MvTarget> getViews() {
        return views;
    }

    public ArrayList<MvInput> getInputs() {
        return inputs;
    }

    public ArrayList<MvIssue> getErrors() {
        return errors;
    }

    public ArrayList<MvIssue> getWarnings() {
        return warnings;
    }

}
