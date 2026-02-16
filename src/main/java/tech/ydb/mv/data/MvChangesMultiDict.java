package tech.ydb.mv.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvPathGenerator;

/**
 * Changes for all dictionaries which are used in the particular handler.
 *
 * @author zinal
 */
public class MvChangesMultiDict {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvChangesMultiDict.class);

    // dictionary table name -> dictionary changes
    private final HashMap<String, MvChangesSingleDict> items = new HashMap<>();

    /**
     * Get dictionary-change items collected for a handler.
     *
     * @return Collection of per-dictionary change objects.
     */
    public Collection<MvChangesSingleDict> getItems() {
        return items.values();
    }

    /**
     * Check whether there are any recorded changes.
     *
     * @return {@code true} if all contained dictionaries have no changes
     * recorded.
     */
    public boolean isEmpty() {
        for (var item : items.values()) {
            if (!item.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Add (or replace) an item for a dictionary table.
     *
     * @param sd Change tracking for a single dictionary table.
     * @return This instance for chaining.
     */
    public MvChangesMultiDict addItem(MvChangesSingleDict sd) {
        items.put(sd.getTableName(), sd);
        return this;
    }

    /**
     * Check if there are any changes in the specified dictionary table.
     *
     * @param tableName Dictionary table name
     * @return true, if there are changes, false otherwise
     */
    public boolean hasKnownChanges(String tableName) {
        var changes = items.get(tableName);
        if (changes == null) {
            return false;
        }
        return !changes.isEmpty();
    }

    /**
     * Build row filters for all view parts impacted by dictionary changes.
     *
     * @param handler Handler definition (views, sources).
     * @return List of non-empty filters to be applied.
     */
    public ArrayList<MvRowFilter> toFilters(MvHandler handler) {
        ArrayList<MvRowFilter> filters = new ArrayList<>(handler.getViews().size());
        for (var view : handler.getViews().values()) {
            for (var target : view.getParts().values()) {
                var filter = toFilter(handler, target);
                if (filter != null && !filter.isEmpty()) {
                    filters.add(filter);
                }
            }
        }
        return filters;
    }

    /**
     * Build a row filter for a single view part, based on relevant dictionary
     * changes.
     *
     * @param handler Handler definition (views, sources).
     * @param target View part to build filter for.
     * @return Filter for {@code target}, or {@code null} if there are no
     * relevant changes.
     */
    public MvRowFilter toFilter(MvHandler handler, MvViewExpr target) {
        // table alias -> keys to be checked
        var dictChecks = new HashMap<String, Set<MvKey>>();
        // table alias -> used columns
        var columnUsage = getColumnUsage(target);
        var dictSources = target.getSources().stream()
                .filter(js -> js.isRelated())
                .filter(js -> js.getInput() != null)
                .filter(js -> js.getInput().isBatchMode())
                .filter(js -> hasKnownChanges(js.getInput().getTableName()))
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
        // IMPORTANT: Iterate in target source order so that pathFilter column order
        // matches dictionary transformation (used by ActionKeysFilter for SQL),
        // which is built from target.getSources() order.
        // For details see MvPathGenerator.makeDictTrans()
        MvPathGenerator.Filter pathFilter = new MvPathGenerator.Filter();
        pathFilter.add(target.getTopMostSource());
        for (var src : target.getSources()) {
            if (src != target.getTopMostSource()
                    && dictChecks.containsKey(src.getTableAlias())) {
                pathFilter.add(src);
            }
        }
        MvViewExpr transformation = new MvPathGenerator(target).applyFilter(pathFilter);
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
                // processing just the relevant tables
                filter.addBlock(position, length, keys);
                position += length;
            }
        }
        return filter;
    }

    /**
     * Get column usage map for a view part.
     *
     * The map is: table alias -&gt; columns that are used as outputs, in join
     * conditions, or in filters.
     *
     * @param target View part to analyze.
     * @return Column usage map.
     */
    public static Map<String, Set<String>> getColumnUsage(MvViewExpr target) {
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
            if (column.isReference()) {
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
        if (columns == null) {
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
