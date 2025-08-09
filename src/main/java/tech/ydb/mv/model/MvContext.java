package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author mzinal
 */
public class MvContext {

    private final HashMap<String, MvTarget> targets = new HashMap<>();
    private final HashMap<String, MvHandler> handlers = new HashMap<>();

    private final ArrayList<MvIssue> errors = new ArrayList<>();
    private final ArrayList<MvIssue> warnings = new ArrayList<>();

    public Map<String, MvTarget> getTargets() {
        return targets;
    }

    public Map<String, MvHandler> getHandlers() {
        return handlers;
    }

    public ArrayList<MvIssue> getErrors() {
        return errors;
    }

    public ArrayList<MvIssue> getWarnings() {
        return warnings;
    }

    public boolean isValid() {
        return errors.isEmpty();
    }

    public void addTarget(MvTarget t) {
        targets.put(t.getName(), t);
    }

    public void addHandler(MvHandler h) {
        handlers.put(h.getName(), h);
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

}
