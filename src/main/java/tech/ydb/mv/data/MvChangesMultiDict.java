package tech.ydb.mv.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvTarget;

/**
 * Changes for all dictionaries which are used in the particular handler.
 *
 * @author zinal
 */
public class MvChangesMultiDict {

    private final HashMap<String, MvChangesSingleDict> items = new HashMap<>();

    public Collection<MvChangesSingleDict> getItems() {
        return Collections.unmodifiableCollection(items.values());
    }

    public MvChangesMultiDict addItem(MvChangesSingleDict sd) {
        items.put(sd.getTableName(), sd);
        return this;
    }

    public ArrayList<MvRowFilter> toFilters(MvHandler handler) {
        ArrayList<MvRowFilter> filters = new ArrayList<>(handler.getTargets().size());
        for (var target : handler.getTargets().values()) {
            var filter = toFilter(handler, target);
            if (! filter.isEmpty()) {
                filters.add(filter);
            }
        }
        return filters;
    }

    public MvRowFilter toFilter(MvHandler handler, MvTarget target) {
        var columnUsage = getColumnUsage(target);
        var dictSources = target.getSources().stream()
                .filter(js -> js.isRelated())
                .filter(js -> js.getInput().isBatchMode())
                .toList();
        for (var dict : dictSources) {
            MvChangesSingleDict change = items.get(dict.getTableName());
            if (change == null) {
                continue;
            }
            for (String fieldName : change.getFields().keySet()) {

            }
        }
        MvRowFilter filter = new MvRowFilter(target);
        return filter;
    }

    /**
     * table alias -> columns being used as output or in relations
     * @param target
     * @return the column usage map
     */
    public static Map<String, Set<String>> getColumnUsage(MvTarget target) {
        HashMap<String, Set<String>> columnUsage = new HashMap<>();
        for (var js : target.getSources()) {
            for (var cond : js.getConditions()) {
                if (cond.getFirstAlias() != null) {
                    useColumn(columnUsage, cond.getFirstAlias(), cond.getFirstColumn());
                }
                if (cond.getSecondAlias() != null) {
                    useColumn(columnUsage, cond.getSecondAlias(), cond.getSecondColumn());
                }
            }
        }
        for (var column : target.getColumns()) {
            if ( column.isReference() ) {
                useColumn(columnUsage, column.getSourceAlias(), column.getSourceColumn());
            } else if (column.isComputation()) {
                for (var src : column.getComputation().getSources()) {
                    useColumn(columnUsage, src.getAlias(), src.getColumn());
                }
            }
        }
        if (target.getFilter() != null) {
            for (var src : target.getFilter().getSources()) {
                useColumn(columnUsage, src.getAlias(), src.getColumn());
            }
        }
        return columnUsage;
    }

    private static void useColumn(Map<String, Set<String>> columnUsage,
            String tableAlias, String columnName) {
        if (tableAlias == null || columnName == null
                || tableAlias.length() == 0 || columnName.length() == 0) {
            return;
        }
        Set<String> columns = columnUsage.get(tableAlias);
        if (columns==null) {
            columns = new HashSet<>();
            columnUsage.put(tableAlias, columns);
        }
        columns.add(columnName);
    }

}
