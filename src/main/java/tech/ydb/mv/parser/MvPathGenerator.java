package tech.ydb.mv.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvLiteral;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvViewExpr;

/**
 * Generates a minimal MvViewExpr that defines the transformation needed to
 * convert the primary key value of a specified MvJoinSource to the primary key
 * value of the destination MvView.
 *
 * The generated MvTarget includes only the minimal set of MvJoinSource
 * instances needed to perform the transformation.
 *
 * @author zinal
 */
public class MvPathGenerator {

    private final MvViewExpr expr;
    private final MvJoinSource topMostSource;
    private final MvTableInfo topMostTable;
    private final Map<MvJoinSource, List<MvJoinSource>> adjacencyMap;

    public MvPathGenerator(MvViewExpr expr) {
        if (expr == null || expr.getSources().isEmpty()) {
            throw new IllegalArgumentException("Input expression is not valid for path generator");
        }
        this.expr = expr;
        this.topMostSource = expr.getTopMostSource();
        this.topMostTable = this.topMostSource.getTableInfo();
        this.adjacencyMap = buildAdjacencyMap(expr);
    }

    public MvViewExpr getExpr() {
        return expr;
    }

    public String getTopSourceTableName() {
        return topMostSource.getTableName();
    }

    /**
     * Generates a minimal transformation target from the input join source to
     * the top-most join source.
     *
     * @param point The input MvJoinSource to transform from
     * @return A new MvTarget defining the minimal transformation, or null if no
     * path exists
     * @throws IllegalArgumentException if parameters are invalid
     */
    public MvViewExpr extractKeysReverse(MvJoinSource point) {
        // Validate that inputSource is part of the originalTarget
        if (!expr.getSources().contains(point)) {
            throw new IllegalArgumentException("Input source must be part of the original target");
        }

        // If input source is already the top-most source, create a simple target
        if (point == topMostSource) {
            return createSimpleTarget(point, point.getTableInfo().getKey());
        }

        // Check if the input source already contains all primary key fields of the top-most source
        if (canDirectlyMapKeys(point)) {
            return createDirectTarget(point, topMostTable.getKey(), true);
        }

        List<MvJoinSource> path = findPath(point, topMostSource);
        if (path == null || path.isEmpty()) {
            return null; // No path found
        }
        return createTarget(path, path.get(0), topMostTable.getKey());
    }

    /**
     * Applies the defined filter to the transformation. The filter allows to
     * skip the unnecessary steps, so that only the required joins are
     * performed.
     *
     * @param filter The list of sources and their destination columns
     * @return Transformation after the filter is applied
     */
    public MvViewExpr applyFilter(Filter filter) {
        if (filter == null || filter.items.isEmpty()) {
            throw new IllegalArgumentException("Empty filter passed");
        }

        Set<MvJoinSource> required = new HashSet<>();
        for (FilterItem item : filter.items) {
            if (!expr.getSources().contains(item.source)) {
                throw new IllegalArgumentException("Filter contains join source "
                        + "`" + item.source.getTableAlias() + "` which is not included "
                        + "in the current target");
            }
            if (required.add(item.source)) {
                var path = findPath(topMostSource, item.source);
                if (path == null || path.isEmpty()) {
                    throw new IllegalArgumentException("Filter contains join source "
                            + "`" + item.source.getTableAlias() + "` which is not "
                            + "a valid destination for the current target");
                }
                required.addAll(path);
            }
        }

        MvViewExpr result = new MvViewExpr("filter");
        // Add all sources
        int index = 0;
        for (MvJoinSource src : expr.getSources()) {
            if (!required.contains(src)) {
                continue;
            }
            MvJoinSource dst = cloneJoinSource(src);
            if (index == 0) {
                dst.setMode(MvJoinMode.MAIN);
            } else {
                // Left join, because we should check all filter positions, and some may be missing
                dst.setMode(MvJoinMode.LEFT);
            }
            result.getSources().add(dst);
            ++index;
        }

        // Add relevant join conditions
        index = 0;
        for (MvJoinSource src : expr.getSources()) {
            if (!required.contains(src)) {
                continue;
            }
            MvJoinSource dst = result.getSources().get(index);
            for (MvJoinCondition cond : src.getConditions()) {
                if (required.contains(cond.getFirstRef())
                        || required.contains(cond.getSecondRef())) {
                    dst.getConditions().add(cond.cloneTo(result));
                }
            }
            ++index;
        }

        // Add the requested columns
        index = 0;
        for (FilterItem item : filter.items) {
            MvJoinSource ref = result.getSourceByAlias(item.source.getTableAlias());
            if (ref == null) {
                throw new IllegalStateException("Could not find reference by name: "
                        + item.source.getTableAlias());
            }
            for (String fieldName : item.fieldNames) {
                MvColumn column = new MvColumn("c" + String.valueOf(index++));
                column.setSourceAlias(ref.getTableAlias());
                column.setSourceRef(ref);
                column.setSourceColumn(fieldName);
                column.setType(ref.getTableInfo().getColumns().get(fieldName));
                if (column.getType() == null) {
                    throw new IllegalArgumentException("Filter requested column"
                            + "`" + fieldName + "` which is missing in the join source "
                            + "`" + item.source.getTableAlias() + "` linked to table "
                            + "`" + item.source.getTableName() + "`");
                }
                result.getColumns().add(column);
            }
        }

        return result;
    }

    /**
     * Create the transformation to grab the keys for dictionary scans.
     *
     * The order of the output columns matches the order of sources in the
     * original MV expression.
     *
     * @return Transformation returning the primary key of the topmost-left
     * table along with the fields for dictionary filter checks. null if there
     * are no BATCH-style inputs.
     */
    public MvViewExpr makeDictTrans() {
        var batchSources = expr.getSources().stream()
                .filter(js -> js.isTableKnown())
                .filter(js -> js.getInput() != null && js.getInput().isBatchMode())
                .filter(js -> js.isRelated())
                .toList();
        if (batchSources.isEmpty()) {
            return null;
        }
        var filter = MvPathGenerator.newFilter();
        filter.add(topMostSource, topMostSource.getKeyColumnNames());
        for (var js : batchSources) {
            filter.add(js, js.getKeyColumnNames());
        }
        return applyFilter(filter);
    }

    /**
     * Checks if the input source can directly map to the target primary key
     * without joins. This analyzes join conditions to find direct mappings
     * between columns or literal values.
     */
    private boolean canDirectlyMapKeys(MvJoinSource inputSource) {
        List<String> targetKeys = topMostTable.getKey();
        for (String targetKey : targetKeys) {
            if (!canMapTargetKey(inputSource, targetKey)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if a specific target key can be mapped directly from the input
     * source by analyzing join conditions. Used by key path generator.
     */
    private boolean canMapTargetKey(MvJoinSource source, String fieldName) {
        if (source == topMostSource
                && source.getTableInfo().getColumns().containsKey(fieldName)) {
            return true;
        }

        // Look through all join conditions in the input source
        for (MvJoinCondition condition : source.getConditions()) {
            if (isMappingCondition(condition, source, topMostSource, fieldName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a join condition provides a mapping for the target field.
     */
    private static boolean isMappingCondition(MvJoinCondition condition,
            MvJoinSource source, MvJoinSource target, String fieldName) {
        if (condition.getFirstLiteral() != null || condition.getSecondLiteral() != null) {
            // Literal conditions never match for field mapping
            return false;
        }

        // Check if condition connects topMostSource column to targetSource field
        if (condition.getFirstRef() == source
                && condition.getSecondRef() == target) {
            if (fieldName.equals(condition.getSecondColumn())) {
                return true;
            }
        }

        if (condition.getFirstRef() == target
                && condition.getSecondRef() == source) {
            if (fieldName.equals(condition.getFirstColumn())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Creates a simple direct target for the case where target source is the
     * top-most source.
     */
    private static MvViewExpr createSimpleTarget(MvJoinSource source, List<String> fieldNames) {
        MvViewExpr result = new MvViewExpr(source.getTableName() + "_simple");
        result.setTableInfo(source.getTableInfo());

        // Add the source as the main source
        MvJoinSource newSource = cloneJoinSource(source);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Add columns for all requested fields
        for (String fieldName : fieldNames) {
            MvColumn column = new MvColumn(fieldName);
            column.setSourceAlias(newSource.getTableAlias());
            column.setSourceColumn(fieldName);
            column.setSourceRef(newSource);
            column.setType(source.getTableInfo().getColumns().get(fieldName));
            result.getColumns().add(column);
        }

        return result;
    }

    /**
     * Creates a direct target that maps fields without any joins.
     */
    private MvViewExpr createDirectTarget(MvJoinSource source, List<String> fieldNames, boolean forward) {
        MvViewExpr result = new MvViewExpr(source.getTableName() + "_direct");
        result.setTableInfo(source.getTableInfo());

        // Add the target source as the main source
        MvJoinSource newSource = cloneJoinSource(source);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Add columns for the requested fields, mapping from join conditions
        for (String fieldName : fieldNames) {
            String sourceColumn = findSourceColumn(source, fieldName, forward);
            if (sourceColumn != null) {
                MvColumn column = new MvColumn(fieldName);
                column.setSourceAlias(newSource.getTableAlias());
                column.setSourceColumn(sourceColumn);
                column.setSourceRef(newSource);
                column.setType(source.getTableInfo().getColumns().get(sourceColumn));
                result.getColumns().add(column);
            } else {
                MvLiteral literalValue = findMappedLiteral(source, fieldName);
                if (literalValue != null) {
                    // Handle literal/constant values
                    MvColumn column = new MvColumn(fieldName);
                    MvLiteral targetValue = result.addLiteral(literalValue.getValue());
                    column.setComputation(new MvComputation(targetValue));
                    // Type will be determined from the target field
                    column.setType(source.getTableInfo().getColumns().get(fieldName));
                    result.getColumns().add(column);
                } else {
                    throw new IllegalStateException("Cannot map column for " + fieldName
                            + " at source " + source.getTableName() + ", target "
                            + topMostSource.getTableName());
                }
            }
        }

        return result;
    }

    /**
     * Creates a transformation target based on the found path to retrieve
     * specific fields.
     */
    private static MvViewExpr createTarget(List<MvJoinSource> path,
            MvJoinSource point, List<String> fieldNames) {
        MvViewExpr result = new MvViewExpr(point.getTableName() + "_full");
        result.setTableInfo(point.getTableInfo());

        // Add sources in the path
        for (int i = 0; i < path.size(); i++) {
            MvJoinSource src = path.get(i);
            MvJoinSource dst = cloneJoinSource(src);
            if (i == 0) {
                dst.setMode(MvJoinMode.MAIN);
            } else {
                // Inner join, because we assume that the path exists
                dst.setMode(MvJoinMode.INNER);
                // Copy the literal conditions for filtering.
                copyLiteralConditions(result, src, dst);
            }
            result.getSources().add(dst);
        }

        // Add the relevant conditions to each source, except the leftmost one
        for (int i = 1; i < path.size(); i++) {
            MvJoinSource src = path.get(i);
            MvJoinSource dst = result.getSources().get(i);
            copyRelationalConditions(path, src, dst, result);
        }

        // Add columns for the requested fields from the desired point
        MvJoinSource pointInResult = result.getSources().get(result.getSources().size() - 1);
        fillTargetColumns(result, pointInResult, fieldNames);

        return result;
    }

    /**
     * Add the desired output columns to the join definition.
     *
     * @param result Join definition
     * @param tableRef Source table reference
     * @param fieldNames List of field names to be added
     */
    private static void fillTargetColumns(MvViewExpr result,
            MvJoinSource tableRef, List<String> fieldNames) {
        for (String fieldName : fieldNames) {
            MvColumn column = new MvColumn(fieldName);
            column.setSourceAlias(tableRef.getTableAlias());
            column.setSourceColumn(fieldName);
            column.setSourceRef(tableRef);
            column.setType(tableRef.getTableInfo().getColumns().get(fieldName));
            result.getColumns().add(column);
        }
    }

    /**
     * Finds the literal value that maps to the target field through join
     * conditions.
     */
    private MvLiteral findMappedLiteral(MvJoinSource source, String fieldName) {
        for (MvJoinCondition condition : source.getConditions()) {
            MvLiteral literal = getLiteralFromCondition(condition, fieldName);
            if (literal != null) {
                return literal;
            }
        }
        return null;
    }

    /**
     * Extracts the literal value from a join condition if it maps to the target
     * field.
     */
    private MvLiteral getLiteralFromCondition(MvJoinCondition condition, String fieldName) {
        if (condition.getSecondLiteral() != null) {
            if (condition.getFirstRef() == topMostSource
                    && fieldName.equals(condition.getFirstColumn())) {
                return condition.getSecondLiteral();
            }
        }
        if (condition.getFirstLiteral() != null) {
            if (condition.getSecondRef() == topMostSource
                    && fieldName.equals(condition.getSecondColumn())) {
                return condition.getFirstLiteral();
            }
        }
        return null;
    }

    /**
     * Clones a MvJoinSource with a new SQL position.
     */
    private static MvJoinSource cloneJoinSource(MvJoinSource original) {
        MvJoinSource clone = new MvJoinSource(original.getSqlPos());
        clone.setTableName(original.getTableName());
        clone.setTableAlias(original.getTableAlias());
        clone.setMode(original.getMode());
        clone.setTableInfo(original.getTableInfo());
        clone.setInput(original.getInput());
        return clone;
    }

    /**
     * Copy all literal conditions from the current level. These are the
     * filtering conditions we need.
     */
    private static void copyLiteralConditions(MvViewExpr result,
            MvJoinSource src, MvJoinSource dst) {
        for (MvJoinCondition cond : src.getConditions()) {
            MvLiteral literal = null;
            String column = null;
            if (cond.getFirstRef() == src) {
                literal = cond.getSecondLiteral();
                column = cond.getFirstColumn();
            } else if (cond.getSecondRef() == src) {
                literal = cond.getFirstLiteral();
                column = cond.getSecondColumn();
            }
            if (literal != null && column != null) {
                MvJoinCondition copy = new MvJoinCondition();
                copy.setFirstRef(dst);
                copy.setFirstAlias(dst.getTableAlias());
                copy.setFirstColumn(column);
                copy.setSecondLiteral(result.addLiteral(literal.getValue()));
                dst.getConditions().add(copy);
            }
        }
    }

    /**
     * Copy the relevant relational conditions from path components to dst. The
     * relevant ones are linked to the src reference, and the other side must be
     * linked below the src in the path.
     */
    private static void copyRelationalConditions(List<MvJoinSource> path,
            MvJoinSource src, MvJoinSource dst, MvViewExpr result) {
        int baseIndex = path.indexOf(src);
        if (baseIndex < 0) {
            throw new IllegalArgumentException("Component " + src
                    + " is not in path " + path);
        }
        for (MvJoinSource cur : path) {
            for (MvJoinCondition cond : cur.getConditions()) {
                MvJoinCondition copy = null;
                if (cond.getFirstRef() == src && cond.getSecondRef() != null) {
                    int refIndex = path.indexOf(cond.getSecondRef());
                    if (refIndex >= 0 && refIndex < baseIndex) {
                        copy = cond.cloneTo(result);
                    }
                } else if (cond.getSecondRef() == src && cond.getFirstRef() != null) {
                    int refIndex = path.indexOf(cond.getFirstRef());
                    if (refIndex >= 0 && refIndex < baseIndex) {
                        copy = cond.cloneTo(result);
                    }
                }
                if (copy != null && !isDuplicateCondition(dst.getConditions(), copy)) {
                    dst.getConditions().add(copy);
                }
            }
        }
    }

    /**
     * Checks if a condition is already present in the list to avoid duplicates.
     */
    private static boolean isDuplicateCondition(ArrayList<MvJoinCondition> conditions,
            MvJoinCondition newCondition) {
        for (MvJoinCondition existing : conditions) {
            if (areConditionsEqual(existing, newCondition)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if two conditions are semantically equal.
     */
    private static boolean areConditionsEqual(MvJoinCondition cond1, MvJoinCondition cond2) {
        boolean v = Objects.equals(cond1.getFirstAlias(), cond2.getFirstAlias())
                && Objects.equals(cond1.getFirstColumn(), cond2.getFirstColumn())
                && Objects.equals(cond1.getSecondAlias(), cond2.getSecondAlias())
                && Objects.equals(cond1.getSecondColumn(), cond2.getSecondColumn())
                && Objects.equals(cond1.getFirstLiteral(), cond2.getFirstLiteral())
                && Objects.equals(cond1.getSecondLiteral(), cond2.getSecondLiteral());
        if (v) {
            return true;
        }

        v = Objects.equals(cond1.getFirstAlias(), cond2.getSecondAlias())
                && Objects.equals(cond1.getFirstColumn(), cond2.getSecondColumn())
                && Objects.equals(cond1.getSecondAlias(), cond2.getFirstAlias())
                && Objects.equals(cond1.getSecondColumn(), cond2.getFirstColumn())
                && Objects.equals(cond1.getFirstLiteral(), cond2.getSecondLiteral())
                && Objects.equals(cond1.getSecondLiteral(), cond2.getFirstLiteral());

        return v;
    }

    /**
     * Finds the source column in inputSource that maps to the target key
     * through join conditions.
     */
    private String findSourceColumn(MvJoinSource source, String fieldName, boolean forward) {
        if (source == topMostSource
                && source.getTableInfo().getColumns().containsKey(fieldName)) {
            return fieldName;
        }

        MvJoinSource j1, j2;
        if (forward) {
            j1 = topMostSource;
            j2 = source;
        } else {
            j1 = source;
            j2 = topMostSource;
        }

        // Check join conditions for column mappings
        for (MvJoinCondition condition : source.getConditions()) {
            String sourceColumn = getSourceColumn(condition, j1, j2, fieldName);
            if (sourceColumn != null) {
                return sourceColumn;
            }
        }

        return null;
    }

    /**
     * Extracts the source column from a join condition if it maps to the target
     * field.
     */
    private static String getSourceColumn(MvJoinCondition condition,
            MvJoinSource source, MvJoinSource target, String fieldName) {
        if (condition.getFirstRef() == target && condition.getSecondRef() == source) {
            if (fieldName.equals(condition.getSecondColumn())) {
                return condition.getFirstColumn();
            }
        }
        if (condition.getFirstRef() == source && condition.getSecondRef() == target) {
            if (fieldName.equals(condition.getFirstColumn())) {
                return condition.getSecondColumn();
            }
        }
        return null;
    }

    /**
     * Finds a path from source to target using BFS.
     */
    private List<MvJoinSource> findPath(MvJoinSource from, MvJoinSource to) {
        if (from == to) {
            return Arrays.asList(from);
        }

        LinkedList<MvJoinSource> queue = new LinkedList<>();
        Map<MvJoinSource, MvJoinSource> parent = new HashMap<>();
        Set<MvJoinSource> visited = new HashSet<>();

        queue.offer(from);
        visited.add(from);
        parent.put(from, null);

        while (!queue.isEmpty()) {
            MvJoinSource current = queue.poll();

            for (MvJoinSource neighbor : adjacencyMap.getOrDefault(current, Collections.emptyList())) {
                if (!visited.contains(neighbor)) {
                    visited.add(neighbor);
                    parent.put(neighbor, current);
                    queue.offer(neighbor);

                    if (neighbor == to) {
                        return reconstructPath(parent, from, to);
                    }
                }
            }
        }

        return null; // No path found
    }

    /**
     * Reconstructs the path from parent mapping.
     */
    private List<MvJoinSource> reconstructPath(Map<MvJoinSource, MvJoinSource> parent,
            MvJoinSource from, MvJoinSource to) {
        List<MvJoinSource> path = new ArrayList<>();
        MvJoinSource current = to;

        while (current != null) {
            path.add(current);
            current = parent.get(current);
        }

        Collections.reverse(path);

        // Validation: ensure the path actually starts from the expected 'from' node
        if (path.isEmpty() || !path.get(0).equals(from)) {
            throw new IllegalStateException("Reconstructed path does not start from expected source: "
                    + "expected=" + (from != null ? from.getTableAlias() : "null")
                    + ", actual=" + (path.isEmpty() ? "empty" : path.get(0).getTableAlias()));
        }

        return path;
    }

    /**
     * Builds an adjacency map representing the join relationships.
     */
    private static Map<MvJoinSource, List<MvJoinSource>> buildAdjacencyMap(MvViewExpr target) {
        Map<MvJoinSource, List<MvJoinSource>> map = new HashMap<>();

        // Initialize map with all sources
        for (MvJoinSource source : target.getSources()) {
            map.put(source, new ArrayList<>());
        }

        // Add connections based on join conditions
        for (MvJoinSource source : target.getSources()) {
            for (MvJoinCondition condition : source.getConditions()) {
                MvJoinSource firstRef = condition.getFirstRef();
                MvJoinSource secondRef = condition.getSecondRef();

                // Handle alias-based references
                if (firstRef == null && condition.getFirstAlias() != null) {
                    firstRef = target.getSourceByAlias(condition.getFirstAlias());
                }
                if (secondRef == null && condition.getSecondAlias() != null) {
                    secondRef = target.getSourceByAlias(condition.getSecondAlias());
                }

                // Add bidirectional connections
                if (firstRef != null && secondRef != null) {
                    map.get(firstRef).add(secondRef);
                    map.get(secondRef).add(firstRef);
                }
            }
        }

        return map;
    }

    public static Filter newFilter() {
        return new Filter();
    }

    public static final class FilterItem {

        public final MvJoinSource source;
        public final String[] fieldNames;

        FilterItem(MvJoinSource source, String[] fieldNames) {
            this.source = source;
            this.fieldNames = fieldNames;
        }

        @Override
        public String toString() {
            return "{" + source + ": " + Arrays.toString(fieldNames) + '}';
        }
    }

    public static final class Filter {

        private final ArrayList<FilterItem> items = new ArrayList<>();

        public ArrayList<FilterItem> getItems() {
            return items;
        }

        public Filter add(MvJoinSource source, String... names) {
            items.add(new FilterItem(source, names));
            return this;
        }

        public Filter add(MvJoinSource source) {
            return add(source, source.getKeyColumnNames());
        }

        @Override
        public String toString() {
            return items.toString();
        }
    }

}
