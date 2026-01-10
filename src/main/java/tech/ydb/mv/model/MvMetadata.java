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

    private String dictionaryConsumer = MvConfig.HANDLER_DICTIONARY;

    private final HashMap<String, MvView> views = new HashMap<>();
    private final HashMap<String, MvHandler> handlers = new HashMap<>();
    private final HashMap<String, MvTableInfo> tables = new HashMap<>();

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

    public Map<String, MvView> getViews() {
        return views;
    }

    public Map<String, MvHandler> getHandlers() {
        return handlers;
    }

    public Map<String, MvTableInfo> getTables() {
        return tables;
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

    public MvView addView(MvView v) {
        return views.put(v.getName(), v);
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
        if (!isValid()) {
            return false;
        }
        boolean valid = new MvValidateBasic(this).validate();
        if (valid && conn != null) {
            valid = new MvValidateSql(this, conn).validate();
        }
        return valid;
    }

    /**
     * Load the missing parts of metadata and perform validation. Table, column
     * and changefeed information is loaded using the helper object passed.
     *
     * @param describer Helper for metadata retrieval
     * @return true, if no errors detected, false otherwise
     */
    public boolean linkAndValidate(MvDescriber describer) {
        if (!isValid()) {
            return false;
        }
        tables.clear();
        for (String tabname : collectTables()) {
            MvTableInfo ti = describer.describeTable(tabname);
            if (ti != null) {
                tables.put(tabname, ti);
            }
        }
        linkTables();
        linkColumns();
        if (!validate(describer.getYdb())) {
            return false;
        }
        views.values().forEach(target -> target.updateUsedColumns());
        return true;
    }

    private TreeSet<String> collectTables() {
        TreeSet<String> ret = new TreeSet<>();
        for (MvView v : views.values()) {
            // target table
            ret.add(v.getName());
            for (var t : v.getParts().values()) {
                // source tables
                for (var r : t.getSources()) {
                    ret.add(r.getTableName());
                }
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

    private void linkTables() {
        for (var v : views.values()) {
            v.setTableInfo(tables.get(v.getName()));
            for (var t : v.getParts().values()) {
                for (var js : t.getSources()) {
                    js.setTableInfo(tables.get(js.getTableName()));
                }
            }
        }
        for (MvHandler h : handlers.values()) {
            for (MvInput i : h.getInputs().values()) {
                i.setTableInfo(tables.get(i.getTableName()));
            }
        }
    }

    private void linkColumns() {
        for (var v : views.values()) {
            MvTableInfo ti = v.getTableInfo();
            if (ti == null) {
                continue;
            }
            for (var t : v.getParts().values()) {
                for (var column : t.getColumns()) {
                    column.setType(ti.getColumns().get(column.getName()));
                    v.addColumnIf(column);
                }
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
        if (!isValid()) {
            throw new IllegalStateException("Cannot subset a non-valid metadata");
        }
        if (handler != handlers.get(handler.getName())) {
            throw new IllegalArgumentException("Input handler is not included in the working set");
        }
        MvMetadata output = new MvMetadata();
        output.setDictionaryConsumer(dictionaryConsumer);
        output.addHandler(handler);
        for (MvView v : handler.getViews().values()) {
            output.addView(v);
        }
        return output;
    }

}
