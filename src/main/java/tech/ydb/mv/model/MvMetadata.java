package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.parser.MvDescriber;
import tech.ydb.mv.parser.MvValidateBasic;
import tech.ydb.mv.parser.MvValidateSql;

/**
 * Aggregated metadata used by the YDB Materializer.
 *
 * @author zinal
 */
public class MvMetadata {

    private String dictionaryConsumer = MvConfig.DICTINARY_HANDLER;

    private final HashMap<String, MvTarget> targets = new HashMap<>();
    private final HashMap<String, MvHandler> handlers = new HashMap<>();

    private final ArrayList<MvIssue> errors = new ArrayList<>();
    private final ArrayList<MvIssue> warnings = new ArrayList<>();

    public MvMetadata() {
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

    /**
     * Perform metadata validation.
     *
     * @param conn YDB connection to test SQL fragments against.
     * @return true, if no errors detected, false otherwise
     */
    public boolean validate(YdbConnector conn) {
        if (! isValid()) {
            return false;
        }
        boolean valid = new MvValidateBasic(this).validate();
        if (valid && conn != null) {
            valid = new MvValidateSql(this, conn).validate();
        }
        return valid;
    }

    /**
     * Load the missing parts of metadata and perform validation.
     * Table, column and changefeed information is loaded using the helper object passed.
     *
     * @param describer Helper for metadata retrieval
     * @return true, if no errors detected, false otherwise
     */
    public boolean linkAndValidate(MvDescriber describer) {
        if (! isValid()) {
            return false;
        }
        HashMap<String, MvTableInfo> info = new HashMap<>();
        for (String tabname : collectTables()) {
            MvTableInfo ti = describer.describeTable(tabname);
            if (ti!=null) {
                info.put(tabname, ti);
            }
        }
        linkTables(info);
        linkColumns();
        if (! validate(describer.getYdb()) ) {
            return false;
        }
        targets.values().forEach(target -> target.updateUsedColumns());
        return true;
    }

    private TreeSet<String> collectTables() {
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

    private void linkTables(HashMap<String, MvTableInfo> info) {
        for (MvTarget t : targets.values()) {
            t.setTableInfo(info.get(t.getName()));
            for (MvJoinSource r : t.getSources()) {
                r.setTableInfo(info.get(r.getTableName()));
            }
        }
        for (MvHandler h : handlers.values()) {
            for (MvInput i : h.getInputs().values()) {
                i.setTableInfo(info.get(i.getTableName()));
            }
        }
    }

    private void linkColumns() {
        for (MvTarget target : targets.values()) {
            MvTableInfo ti = target.getTableInfo();
            if (ti==null) {
                continue;
            }
            for (MvColumn column : target.getColumns()) {
                column.setType(ti.getColumns().get(column.getName()));
            }
        }
    }


    /**
     * Create a new MvMetadata instance as a subset of the current one.
     *
     * @param handler The handler which should be available in the subset
     * @return MvMetadata instance limited to the specified handler only
     */
    public MvMetadata subset(MvHandler handler) {
        if (! isValid()) {
            throw new IllegalStateException("Cannot subset a non-valid metadata");
        }
        if (handler != handlers.get(handler.getName())) {
            throw new IllegalArgumentException("Input handler is not included in the working set");
        }
        MvMetadata output = new MvMetadata();
        output.setDictionaryConsumer(dictionaryConsumer);
        output.addHandler(handler);
        for (MvTarget target : handler.getTargets().values()) {
            output.addTarget(target);
        }
        return output;
    }

}
