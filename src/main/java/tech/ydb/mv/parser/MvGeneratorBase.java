package tech.ydb.mv.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;

import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvLiteral;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;

/**
 * Base class for MvTarget generators providing common functionality.
 *
 * @author zinal
 */
abstract class MvGeneratorBase {

    protected final MvTarget originalTarget;
    protected final MvJoinSource topMostSource;
    protected final MvTableInfo topMostTable;
    protected final Map<MvJoinSource, List<MvJoinSource>> adjacencyMap;

    protected MvGeneratorBase(MvTarget target) {
        if (target == null || target.getSources().isEmpty()) {
            throw new IllegalArgumentException("Target is not valid for generator");
        }
        this.originalTarget = target;
        this.topMostSource = target.getTopMostSource();
        this.topMostTable = this.topMostSource.getTableInfo();
        this.adjacencyMap = buildAdjacencyMap(target);
    }

    public MvTarget getOriginalTarget() {
        return originalTarget;
    }

    public MvJoinSource getTopMostSource() {
        return topMostSource;
    }

    public MvTableInfo getTopMostTable() {
        return topMostTable;
    }

    /**
     * Builds an adjacency map representing the join relationships.
     */
    protected static Map<MvJoinSource, List<MvJoinSource>> buildAdjacencyMap(MvTarget target) {
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

    /**
     * Finds a path from source to target using BFS.
     */
    protected List<MvJoinSource> findPath(MvJoinSource from, MvJoinSource to) {
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
    protected List<MvJoinSource> reconstructPath(Map<MvJoinSource, MvJoinSource> parent,
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
     * Clones a MvJoinSource with a new SQL position.
     */
    protected static MvJoinSource cloneJoinSource(MvJoinSource original) {
        MvJoinSource clone = new MvJoinSource(original.getSqlPos());
        clone.setTableName(original.getTableName());
        clone.setTableAlias(original.getTableAlias());
        clone.setMode(original.getMode());
        clone.setTableInfo(original.getTableInfo());
        clone.setInput(original.getInput());
        return clone;
    }

    /**
     * Copy all literal conditions from the current level.
     * These are the filtering conditions we need.
     */
    protected static void copyLiteralConditions(MvTarget result,
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
     * Checks if a condition is already present in the list to avoid duplicates.
     */
    protected static boolean isDuplicateCondition(ArrayList<MvJoinCondition> conditions,
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
    protected static boolean areConditionsEqual(MvJoinCondition cond1, MvJoinCondition cond2) {
        boolean forward = equalStrings(cond1.getFirstAlias(), cond2.getFirstAlias())
                && equalStrings(cond1.getFirstColumn(), cond2.getFirstColumn())
                && equalStrings(cond1.getSecondAlias(), cond2.getSecondAlias())
                && equalStrings(cond1.getSecondColumn(), cond2.getSecondColumn())
                && equalLiterals(cond1.getFirstLiteral(), cond2.getFirstLiteral())
                && equalLiterals(cond1.getSecondLiteral(), cond2.getSecondLiteral());

        boolean reverse = equalStrings(cond1.getFirstAlias(), cond2.getSecondAlias())
                && equalStrings(cond1.getFirstColumn(), cond2.getSecondColumn())
                && equalStrings(cond1.getSecondAlias(), cond2.getFirstAlias())
                && equalStrings(cond1.getSecondColumn(), cond2.getFirstColumn())
                && equalLiterals(cond1.getFirstLiteral(), cond2.getSecondLiteral())
                && equalLiterals(cond1.getSecondLiteral(), cond2.getFirstLiteral());

        return forward || reverse;
    }

    /**
     * Helper method to compare strings safely (handling nulls).
     */
    protected static boolean equalStrings(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return true;
        }
        if (s1 == null || s2 == null) {
            return false;
        }
        return s1.equals(s2);
    }

    /**
     * Helper method to compare literals safely (handling nulls).
     */
    protected static boolean equalLiterals(MvLiteral lit1, MvLiteral lit2) {
        if (lit1 == null && lit2 == null) {
            return true;
        }
        if (lit1 == null || lit2 == null) {
            return false;
        }
        return lit1.equals(lit2);
    }

    /**
     * Checks if a specific target field can be mapped directly from the top-most source
     * by analyzing join conditions. Used by field path generator.
     */
    protected boolean canMapTargetField(MvJoinSource source, String fieldName) {
        if (source == topMostSource
                && source.getTableInfo().getColumns().containsKey(fieldName)) {
            return true;
        }

        // Look through all join conditions in the target source
        for (MvJoinCondition condition : source.getConditions()) {
            if (isMappingCondition(condition, topMostSource, source, fieldName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a specific target key can be mapped directly from the input source
     * by analyzing join conditions. Used by key path generator.
     */
    protected boolean canMapTargetKey(MvJoinSource source, String fieldName) {
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
    protected static boolean isMappingCondition(MvJoinCondition condition,
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
     * Finds the source column in inputSource that maps to the target key
     * through join conditions.
     */
    protected String findSourceColumn(MvJoinSource source, String fieldName, boolean forward) {
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
     * Extracts the source column from a join condition if it maps to the target field.
     */
    protected static String getSourceColumn(MvJoinCondition condition,
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
     * Creates a direct target that maps fields without any joins.
     */
    protected MvTarget createDirectTarget(MvJoinSource source, List<String> fieldNames, boolean forward) {
        MvTarget result = new MvTarget(source.getTableName() + "_direct");
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
                MvLiteral literalValue = findLiteral(source, fieldName);
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
     * Creates a transformation target based on the found path to retrieve specific fields.
     */
    protected static MvTarget createTarget(List<MvJoinSource> path,
            MvJoinSource original, List<String> fieldNames) {
        MvTarget result = new MvTarget(original.getTableName() + "_full");
        result.setTableInfo(original.getTableInfo());

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

        // Add the relevant conditions to each source
        for (int i = 0; i < path.size(); i++) {
            MvJoinSource src = path.get(i);
            MvJoinSource dst = result.getSources().get(i);
            if (i > 0) {
                for (int j = i - 1; j >= 0; --j) {
                    MvJoinSource cur = path.get(j);
                    copyRelationalConditions(cur, src, dst, result);
                }
            }
        }

        // Add columns for the requested fields from the target source
        MvJoinSource targetSourceInResult = result.getSources().get(result.getSources().size() - 1);
        fillTargetColumns(result, targetSourceInResult, fieldNames);

        return result;
    }

    /**
     * Finds the literal value that maps to the target field through join
     * conditions.
     */
    protected MvLiteral findLiteral(MvJoinSource source, String fieldName) {
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
    protected MvLiteral getLiteralFromCondition(MvJoinCondition condition, String fieldName) {
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
     * Copy the relevant relational conditions from cur to dst.
     * The relevant ones are linked to the src reference.
     */
    protected static void copyRelationalConditions(MvJoinSource cur,
            MvJoinSource src, MvJoinSource dst, MvTarget result) {
        for (MvJoinCondition cond : cur.getConditions()) {
            MvJoinCondition copy = null;
            if (cond.getFirstRef()==src && cond.getSecondRef()!=null) {
                copy = new MvJoinCondition(cond.getSqlPos());
                copy.setFirstRef(dst);
                copy.setFirstAlias(dst.getTableAlias());
                copy.setFirstColumn(cond.getFirstColumn());
                copy.setSecondRef(result.getSourceByAlias(cond.getSecondAlias()));
                copy.setSecondAlias(cond.getSecondAlias());
                copy.setSecondColumn(cond.getSecondColumn());
            } else if (cond.getSecondRef()==src && cond.getFirstRef()!=null) {
                copy = new MvJoinCondition(cond.getSqlPos());
                copy.setFirstRef(dst);
                copy.setFirstAlias(dst.getTableAlias());
                copy.setFirstColumn(cond.getSecondColumn());
                copy.setSecondRef(result.getSourceByAlias(cond.getFirstAlias()));
                copy.setSecondAlias(cond.getFirstAlias());
                copy.setSecondColumn(cond.getFirstColumn());
            }
            if (copy!=null && !isDuplicateCondition(dst.getConditions(), copy)) {
                dst.getConditions().add(copy);
            }
        }
    }

    /**
     * Add the desired output columns to the join definition.
     *
     * @param result Join definition
     * @param tableRef Source table reference
     * @param fieldNames List of field names to be added
     */
    protected static void fillTargetColumns(MvTarget result,
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

}
