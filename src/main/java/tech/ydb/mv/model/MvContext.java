package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvContext {

    private String dictionaryConsumer = MvConfig.DICTINARY_HANDLER;

    private final HashMap<String, MvTarget> targets = new HashMap<>();
    private final HashMap<String, MvHandler> handlers = new HashMap<>();

    private final ArrayList<MvIssue> errors = new ArrayList<>();
    private final ArrayList<MvIssue> warnings = new ArrayList<>();

    public MvContext() {
    }

    public String getDictionaryConsumer() {
        return dictionaryConsumer;
    }

    public void setDictionaryConsumer(String dictionaryConsumer) {
        this.dictionaryConsumer = dictionaryConsumer;
    }

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

    public MvTarget addTarget(MvTarget t) {
        return targets.put(t.getName(), t);
    }

    public MvHandler addHandler(MvHandler h) {
        return handlers.put(h.getName(), h);
    }

    public TreeSet<String> collectTables() {
        TreeSet<String> ret = new TreeSet<>();
        for (MvTarget t : targets.values()) {
            // target table
            ret.add(t.getName());
            // source tables
            for (MvJoinSource r : t.getSources()) {
                ret.add(r.getTableName());
            }
        }
        // possible extra inputs (which may be missing in the targets)
        for (MvHandler h : handlers.values()) {
            for (MvInput i : h.getInputs().values()) {
                ret.add(i.getTableName());
            }
        }
        return ret;
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
