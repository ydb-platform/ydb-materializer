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
import tech.ydb.mv.parser.MvPathGenerator;

/**
 * Changes for all dictionaries which are used in the particular handler.
 *
 * @author zinal
 */
public class MvChangesMultiDict {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvChangesMultiDict.class);

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
            if (filter != null && !filter.isEmpty()) {
                filters.add(filter);
            }
        }
        return filters;
    }

    public MvRowFilter toFilter(MvHandler handler, MvTarget target) {
        // table alias -> keys to be checked
        var dictChecks = new HashMap<String, Set<MvKey>>();
        // table alias -> used columns
        var columnUsage = getColumnUsage(target);
        var dictSources = target.getSources().stream()
                .filter(js -> js.isRelated())
                .filter(js -> js.getInput().isBatchMode())
                .toList();
        // Collect key changes per dictionary source
        for (var dict : dictSources) {
            MvChangesSingleDict change = items.get(dict.getTableName());
            if (change == null) {
                continue;
            }
            var currentColumnUsage = columnUsage.get(dict.getTableAlias());
            for (var entry : change.getFields().entrySet()) {
                if (currentColumnUsage.contains(entry.getKey())) {
                    // the updated column is used in the MV
                    addChecks(dictChecks, dict.getTableAlias(), entry.getValue());
                }
            }
        }
        if (dictChecks.isEmpty()) {
            // No important changes.
            return null;
        }

        // Build the desired transformation to return the keys.
        MvPathGenerator.Filter pathFilter = new MvPathGenerator.Filter();
        pathFilter.add(target.getTopMostSource());
        for (var tableAlias : dictChecks.keySet()) {
            pathFilter.add(target.getSourceByAlias(tableAlias));
        }
        MvTarget transformation = new MvPathGenerator(target).applyFilter(pathFilter);
        if (transformation == null) {
            LOG.error("DICTIONARY CHANGES LOST. Unable to build transformation "
                    + "for target {}, filter {}", target, pathFilter);
            return null;
        }

        MvRowFilter filter = new MvRowFilter(target);
        filter.setTransformation(transformation);
        int position = target.getTopMostSource().getTableInfo().getKey().size();
        for (var item : pathFilter.getItems()) {
            int length = item.source.getTableInfo().getKey().size();
            var keys = dictChecks.get(item.source.getTableAlias());
            if (keys != null) {
                filter.addBlock(position, length, keys);
            }
            position += length;
        }
        return filter;
    }

    /**
     * table alias -> columns being used as output or in relations.
     * // TODO: move to MvTarget to be collected after parsing
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

    private static void addChecks(Map<String, Set<MvKey>> dictChecks,
            String tableAlias, Set<MvKey> changes) {
        if (tableAlias == null || tableAlias.length() == 0
                || changes == null || changes.isEmpty()) {
            return;
        }
        Set<MvKey> keys = dictChecks.get(tableAlias);
        if (keys == null) {
            keys = new HashSet<>();
            dictChecks.put(tableAlias, keys);
        }
        keys.addAll(changes);
    }

}
