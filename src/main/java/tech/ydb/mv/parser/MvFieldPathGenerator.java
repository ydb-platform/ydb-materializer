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
 * Generates a minimal MvTarget that defines the transformation needed to
 * obtain specific fields of a specific table, given the primary key of the
 * top-most-left table in the original MvTarget.
 *
 * The generated MvTarget includes only the minimal set of MvJoinSource
 * instances needed to perform the transformation and retrieve the requested fields.
 *
 * @author zinal
 */
public class MvFieldPathGenerator {

    private final MvTarget originalTarget;
    private final MvJoinSource topMostSource;
    private final MvTableInfo topMostTable;
    private final Map<MvJoinSource, List<MvJoinSource>> adjacencyMap;

    public MvFieldPathGenerator(MvTarget target) {
        if (target == null || target.getSources().isEmpty()) {
            throw new IllegalArgumentException("Target is not valid for field path generator");
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
     * Generates a transformation target to obtain specific fields from a target table,
     * given the primary key of the top-most table.
     *
     * @param targetTableAlias The alias of the table to retrieve fields from
     * @param fieldNames The names of the fields to retrieve
     * @return A new MvTarget defining the minimal transformation, or null if no path exists
     * @throws IllegalArgumentException if parameters are invalid
     */
    public MvTarget generate(String targetTableAlias, List<String> fieldNames) {
        if (targetTableAlias == null || fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalArgumentException("Target table alias and field names must be provided");
        }

        // Find the target source by alias
        MvJoinSource targetSource = originalTarget.getSourceByAlias(targetTableAlias);
        if (targetSource == null) {
            throw new IllegalArgumentException("Target table alias '" + targetTableAlias + "' not found in original target");
        }

        // Validate that all requested fields exist in the target table
        MvTableInfo targetTableInfo = targetSource.getTableInfo();
        if (targetTableInfo == null) {
            throw new IllegalArgumentException("Table info not available for target table '" + targetTableAlias + "'");
        }

        for (String fieldName : fieldNames) {
            if (!targetTableInfo.getColumns().containsKey(fieldName)) {
                throw new IllegalArgumentException("Field '" + fieldName + "' not found in table '" + targetTableAlias + "'");
            }
        }

        // If target source is the top-most source, create a simple target
        if (targetSource == topMostSource) {
            return createSimpleFieldTarget(targetSource, fieldNames);
        }

        // Check if we can directly map the fields without joins
        if (canDirectlyMapFields(targetSource, fieldNames)) {
            return createDirectFieldTarget(targetSource, fieldNames);
        }

        // Find path from top-most source to target source
        List<MvJoinSource> path = findPath(topMostSource, targetSource);
        if (path == null || path.isEmpty()) {
            return null; // No path found
        }

        return createFieldTarget(path, targetSource, fieldNames);
    }

    /**
     * Generates a transformation target to obtain all fields from a target table,
     * given the primary key of the top-most table.
     *
     * @param targetTableAlias The alias of the table to retrieve fields from
     * @return A new MvTarget defining the minimal transformation, or null if no path exists
     * @throws IllegalArgumentException if parameters are invalid
     */
    public MvTarget generateAllFields(String targetTableAlias) {
        MvJoinSource targetSource = originalTarget.getSourceByAlias(targetTableAlias);
        if (targetSource == null) {
            throw new IllegalArgumentException("Target table alias '" + targetTableAlias + "' not found in original target");
        }

        MvTableInfo targetTableInfo = targetSource.getTableInfo();
        if (targetTableInfo == null) {
            throw new IllegalArgumentException("Table info not available for target table '" + targetTableAlias + "'");
        }

        List<String> allFields = new ArrayList<>(targetTableInfo.getColumns().keySet());
        return generate(targetTableAlias, allFields);
    }

    /**
     * Checks if the target fields can be directly mapped from the top-most source
     * without joins by analyzing join conditions.
     */
    private boolean canDirectlyMapFields(MvJoinSource targetSource, List<String> fieldNames) {
        for (String fieldName : fieldNames) {
            if (!canMapTargetField(targetSource, fieldName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if a specific target field can be mapped directly from the top-most source
     * by analyzing join conditions.
     */
    private boolean canMapTargetField(MvJoinSource targetSource, String fieldName) {
        if (targetSource == topMostSource
                && targetSource.getTableInfo().getColumns().containsKey(fieldName)) {
            return true;
        }

        // Look through all join conditions in the target source
        for (MvJoinCondition condition : targetSource.getConditions()) {
            if (isConditionMappingField(condition, targetSource, fieldName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a join condition provides a mapping for the target field.
     */
    private boolean isConditionMappingField(MvJoinCondition condition,
            MvJoinSource targetSource, String fieldName) {
        if (condition.getFirstLiteral() != null || condition.getSecondLiteral() != null) {
            // Literal conditions never match for field mapping
            return false;
        }

        // Check if condition connects topMostSource column to targetSource field
        if (condition.getFirstRef() == topMostSource
                && condition.getSecondRef() == targetSource) {
            if (fieldName.equals(condition.getSecondColumn())) {
                // topMostSource.firstColumn = targetSource.fieldName
                return true;
            }
        }

        if (condition.getFirstRef() == targetSource
                && condition.getSecondRef() == topMostSource) {
            if (fieldName.equals(condition.getFirstColumn())) {
                // targetSource.fieldName = topMostSource.secondColumn
                return true;
            }
        }

        return false;
    }

    /**
     * Creates a simple direct target for the case where target source
     * is the top-most source.
     */
    private MvTarget createSimpleFieldTarget(MvJoinSource source, List<String> fieldNames) {
        MvTarget result = new MvTarget(source.getTableName() + "_fields_simple", source.getSqlPos());
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
    private MvTarget createDirectFieldTarget(MvJoinSource source, List<String> fieldNames) {
        MvTarget result = new MvTarget(source.getTableName() + "_fields_direct");
        result.setTableInfo(source.getTableInfo());

        // Add the target source as the main source
        MvJoinSource newSource = cloneJoinSource(source);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Add columns for the requested fields, mapping from join conditions
        for (String fieldName : fieldNames) {
            String sourceColumn = findSourceColumnForField(source, fieldName);
            if (sourceColumn != null) {
                MvColumn column = new MvColumn(fieldName);
                column.setSourceAlias(newSource.getTableAlias());
                column.setSourceColumn(sourceColumn);
                column.setSourceRef(newSource);
                column.setType(source.getTableInfo().getColumns().get(sourceColumn));
                result.getColumns().add(column);
            } else {
                MvLiteral literalValue = findLiteralForField(source, fieldName);
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
     * Finds the source column in targetSource that maps to the target field
     * through join conditions.
     */
    private String findSourceColumnForField(MvJoinSource targetSource, String fieldName) {
        if (targetSource == topMostSource
                && targetSource.getTableInfo().getColumns().containsKey(fieldName)) {
            return fieldName;
        }

        // Check join conditions for column mappings
        for (MvJoinCondition condition : targetSource.getConditions()) {
            String sourceColumn = getSourceColumnFromCondition(condition, targetSource, fieldName);
            if (sourceColumn != null) {
                return sourceColumn;
            }
        }

        return null;
    }

    /**
     * Finds the literal value that maps to the target field through join
     * conditions.
     */
    private MvLiteral findLiteralForField(MvJoinSource targetSource, String fieldName) {
        for (MvJoinCondition condition : targetSource.getConditions()) {
            MvLiteral literal = getLiteralFromCondition(condition, fieldName);
            if (literal != null) {
                return literal;
            }
        }
        return null;
    }

    /**
     * Extracts the source column from a join condition if it maps to the target field.
     */
    private String getSourceColumnFromCondition(MvJoinCondition condition,
            MvJoinSource targetSource, String fieldName) {
        if (condition.getFirstRef() == topMostSource && condition.getSecondRef() == targetSource) {
            if (fieldName.equals(condition.getSecondColumn())) {
                return condition.getFirstColumn();
            }
        }
        if (condition.getFirstRef() == targetSource && condition.getSecondRef() == topMostSource) {
            if (fieldName.equals(condition.getFirstColumn())) {
                return condition.getSecondColumn();
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
     * Creates a transformation target based on the found path to retrieve specific fields.
     */
    private MvTarget createFieldTarget(List<MvJoinSource> path, MvJoinSource targetSource, List<String> fieldNames) {
        MvTarget result = new MvTarget(targetSource.getTableName() + "_fields_full");
        result.setTableInfo(targetSource.getTableInfo());

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
                // Copy conditions from the current source that connect it to previous sources
                copyRelationalConditions(src, dst, result);
            }
        }

        // Add columns for the requested fields from the target source
        MvJoinSource targetSourceInResult = result.getSources().get(result.getSources().size() - 1);
        for (String fieldName : fieldNames) {
            MvColumn column = new MvColumn(fieldName);
            column.setSourceAlias(targetSourceInResult.getTableAlias());
            column.setSourceColumn(fieldName);
            column.setSourceRef(targetSourceInResult);
            column.setType(targetSource.getTableInfo().getColumns().get(fieldName));
            result.getColumns().add(column);
        }

        return result;
    }

    /**
     * Copy all literal conditions from the current level.
     * These are the filtering conditions we need.
     */
    private void copyLiteralConditions(MvTarget result, MvJoinSource src, MvJoinSource dst) {
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
     * Copy the relevant relational conditions from src to dst.
     * The relevant ones connect src to sources that are already in the result.
     */
    private void copyRelationalConditions(MvJoinSource src, MvJoinSource dst, MvTarget result) {
        for (MvJoinCondition cond : src.getConditions()) {
            MvJoinCondition copy = null;
            if (cond.getFirstRef() == src && cond.getSecondRef() != null) {
                // Check if the second reference is already in our result
                MvJoinSource secondRefInResult = result.getSourceByAlias(cond.getSecondAlias());
                if (secondRefInResult != null) {
                    copy = new MvJoinCondition(cond.getSqlPos());
                    copy.setFirstRef(dst);
                    copy.setFirstAlias(dst.getTableAlias());
                    copy.setFirstColumn(cond.getFirstColumn());
                    copy.setSecondRef(secondRefInResult);
                    copy.setSecondAlias(cond.getSecondAlias());
                    copy.setSecondColumn(cond.getSecondColumn());
                }
            } else if (cond.getSecondRef() == src && cond.getFirstRef() != null) {
                // Check if the first reference is already in our result
                MvJoinSource firstRefInResult = result.getSourceByAlias(cond.getFirstAlias());
                if (firstRefInResult != null) {
                    copy = new MvJoinCondition(cond.getSqlPos());
                    copy.setFirstRef(firstRefInResult);
                    copy.setFirstAlias(cond.getFirstAlias());
                    copy.setFirstColumn(cond.getFirstColumn());
                    copy.setSecondRef(dst);
                    copy.setSecondAlias(dst.getTableAlias());
                    copy.setSecondColumn(cond.getSecondColumn());
                }
            }
            if (copy != null && !isDuplicateCondition(dst.getConditions(), copy)) {
                dst.getConditions().add(copy);
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
    private static boolean equalStrings(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return true;
        }
        if (s1 == null || s2 == null) {
            return false;
        }
        return s1.equals(s2);
    }

    private static boolean equalLiterals(MvLiteral lit1, MvLiteral lit2) {
        if (lit1 == null && lit2 == null) {
            return true;
        }
        if (lit1 == null || lit2 == null) {
            return false;
        }
        return lit1.equals(lit2);
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
     * Builds an adjacency map representing the join relationships.
     */
    private static Map<MvJoinSource, List<MvJoinSource>> buildAdjacencyMap(MvTarget target) {
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
}
