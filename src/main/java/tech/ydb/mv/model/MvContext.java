package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @author mzinal
 */
public class MvContext {

    private final ArrayList<MvTarget> targets = new ArrayList<>();
    private final ArrayList<MvInput> inputs = new ArrayList<>();

    private final ArrayList<MvIssue> errors = new ArrayList<>();
    private final ArrayList<MvIssue> warnings = new ArrayList<>();

    public boolean isValid() {
        return errors.isEmpty();
    }

    public void addIssue(MvIssue i) {
        if (i.isError()) {
            errors.add(i);
        } else {
            warnings.add(i);
        }
    }

    public void addIssues(Collection<? extends MvIssue> ix) {
        for (MvIssue i : ix) {
            addIssue(i);
        }
    }

    public ArrayList<MvTarget> getTargets() {
        return targets;
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
