package tech.ydb.mv.apply;

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
 * convert the primary key value of a specified MvJoinSource to the primary key
 * value of the top-most MvJoinSource in the original MvTarget.
 *
 * The generated MvTarget includes only the minimal set of MvJoinSource
 * instances needed to perform the transformation.
 *
 * @author zinal
 */
public class MvKeyPathGenerator {

    private final MvTarget originalTarget;
    private final MvJoinSource topMostSource;
    private final MvTableInfo topMostTable;
    private final Map<MvJoinSource, List<MvJoinSource>> adjacencyMap;

    public MvKeyPathGenerator(MvTarget target) {
        this.originalTarget = target;
        this.topMostSource = target.getSources().get(0);
        this.topMostTable = this.topMostSource.getTableInfo();
        this.adjacencyMap = buildAdjacencyMap(target);
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
    public MvTarget generate(MvJoinSource point) {
        // Validate that inputSource is part of the originalTarget
        if (!originalTarget.getSources().contains(point)) {
            throw new IllegalArgumentException("Input source must be part of the original target");
        }

        // If input source is already the top-most source, create a simple target
        if (point == topMostSource) {
            return createSimpleTarget(point);
        }

        // Check if the input source already contains all primary key fields of the top-most source
        if (canDirectlyMapKeys(point)) {
            return createDirectTarget(point);
        }

        List<MvJoinSource> path = findPath(point, topMostSource);
        if (path == null || path.isEmpty()) {
            return null; // No path found
        }
        return createTarget(path);
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
     * source by analyzing join conditions.
     */
    private boolean canMapTargetKey(MvJoinSource inputSource, String targetKey) {
        if (inputSource == topMostSource
                && inputSource.getTableInfo().getColumns().containsKey(targetKey)) {
            return true;
        }

        // Look through all join conditions in the input source
        for (MvJoinCondition condition : inputSource.getConditions()) {
            if (isConditionMappingKey(condition, inputSource, targetKey)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a join condition provides a mapping for the target key.
     */
    private boolean isConditionMappingKey(MvJoinCondition condition,
            MvJoinSource inputSource, String targetKey) {
        if (condition.getFirstLiteral() != null || condition.getSecondLiteral() != null) {
            // Literal conditions never match
            return false;
        }

        // Check if condition connects inputSource column to topMostSource key
        if (condition.getFirstRef() == inputSource
                && condition.getSecondRef() == topMostSource) {
            if (targetKey.equals(condition.getSecondColumn())) {
                // inputSource.firstColumn = topMostSource.targetKey
                return true;
            }
        }

        if (condition.getFirstRef() == topMostSource
                && condition.getSecondRef() == inputSource) {
            if (targetKey.equals(condition.getFirstColumn())) {
                // topMostSource.targetKey = inputSource.secondColumn
                return true;
            }
        }

        return false;
    }

    /**
     * Creates a simple direct target for the case where input source
     * is the top-most source.
     */
    private MvTarget createSimpleTarget(MvJoinSource source) {
        MvTarget result = new MvTarget(source.getTableName() + "_keys_simple");

        // Add the source as the main source
        MvJoinSource newSource = cloneJoinSource(source);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Add columns for all primary key fields
        for (String keyColumn : source.getTableInfo().getKey()) {
            MvColumn column = new MvColumn(keyColumn);
            column.setSourceAlias(newSource.getTableAlias());
            column.setSourceColumn(keyColumn);
            column.setSourceRef(newSource);
            column.setType(source.getTableInfo().getColumns().get(keyColumn));
            result.getColumns().add(column);
        }

        return result;
    }

    /**
     * Creates a direct target that maps keys without any joins.
     */
    private MvTarget createDirectTarget(MvJoinSource source) {
        MvTarget result = new MvTarget(source.getTableName() + "_keys_direct");

        // Add the input source as the main source
        MvJoinSource newSource = cloneJoinSource(source);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Add columns for the target primary key fields, mapping from join conditions
        List<String> targetKeys = topMostTable.getKey();

        for (String targetKey : targetKeys) {
            String sourceColumn = findSourceColumnForKey(source, targetKey);
            if (sourceColumn != null) {
                MvColumn column = new MvColumn(targetKey);
                column.setSourceAlias(newSource.getTableAlias());
                column.setSourceColumn(sourceColumn);
                column.setSourceRef(newSource);
                column.setType(source.getTableInfo().getColumns().get(sourceColumn));
                result.getColumns().add(column);
            } else {
                MvLiteral literalValue = findLiteralForKey(source, targetKey);
                if (literalValue != null) {
                    // Handle literal/constant values
                    MvColumn column = new MvColumn(targetKey);
                    MvLiteral targetValue = result.addLiteral(literalValue.getValue());
                    column.setComputation(new MvComputation(targetValue));
                    // Type will be determined from the target key
                    column.setType(topMostTable.getColumns().get(targetKey));
                    result.getColumns().add(column);
                } else {
                    throw new IllegalStateException("Cannot map column for " + targetKey
                            + " at source " + source.getTableName() + ", target "
                            + topMostSource.getTableName());
                }
            }
        }

        return result;
    }

    /**
     * Finds the source column in inputSource that maps to the target key
     * through join conditions.
     */
    private String findSourceColumnForKey(MvJoinSource inputSource, String targetKey) {
        if (inputSource == topMostSource
                && inputSource.getTableInfo().getColumns().containsKey(targetKey)) {
            return targetKey;
        }

        // Check join conditions for column mappings
        for (MvJoinCondition condition : inputSource.getConditions()) {
            String sourceColumn = getSourceColumnFromCondition(condition, inputSource, targetKey);
            if (sourceColumn != null) {
                return sourceColumn;
            }
        }

        return null;
    }

    /**
     * Finds the literal value that maps to the target key through join
     * conditions.
     */
    private MvLiteral findLiteralForKey(MvJoinSource inputSource, String targetKey) {
        for (MvJoinCondition condition : inputSource.getConditions()) {
            MvLiteral literal = getLiteralFromCondition(condition, targetKey);
            if (literal != null) {
                return literal;
            }
        }
        return null;
    }

    /**
     * Extracts the source column from a join condition if it maps to the target key.
     */
    private String getSourceColumnFromCondition(MvJoinCondition condition,
            MvJoinSource inputSource, String targetKey) {
        if (condition.getFirstRef() == inputSource && condition.getSecondRef() == topMostSource) {
            if (targetKey.equals(condition.getSecondColumn())) {
                return condition.getFirstColumn();
            }
        }
        if (condition.getFirstRef() == topMostSource && condition.getSecondRef() == inputSource) {
            if (targetKey.equals(condition.getFirstColumn())) {
                return condition.getSecondColumn();
            }
        }
        return null;
    }

    /**
     * Extracts the literal value from a join condition if it maps to the target
     * key.
     */
    private MvLiteral getLiteralFromCondition(MvJoinCondition condition, String targetKey) {
        if (condition.getSecondLiteral() != null) {
            if (condition.getFirstRef() == topMostSource
                    && targetKey.equals(condition.getFirstColumn())) {
                return condition.getSecondLiteral();
            }
        }
        if (condition.getFirstLiteral() != null) {
            if (condition.getSecondRef() == topMostSource
                    && targetKey.equals(condition.getSecondColumn())) {
                return condition.getFirstLiteral();
            }
        }
        return null;
    }

    /**
     * Creates a transformation target based on the found path.
     */
    private MvTarget createTarget(List<MvJoinSource> path) {
        MvTarget result = new MvTarget(path.get(0).getTableName() + "_keys_full");

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

        // Add columns for the target primary key (top-most source key)
        MvJoinSource topmost = result.getSources().get(result.getSources().size() - 1);
        for (String keyColumn : topMostTable.getKey()) {
            MvColumn column = new MvColumn(keyColumn);
            column.setSourceAlias(topmost.getTableAlias());
            column.setSourceColumn(keyColumn);
            column.setSourceRef(topmost);
            column.setType(topMostTable.getColumns().get(keyColumn));
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
            if (cond.getFirstRef()==src) {
                literal = cond.getSecondLiteral();
                column = cond.getFirstColumn();
            } else if (cond.getSecondRef()==src) {
                literal = cond.getFirstLiteral();
                column = cond.getSecondColumn();
            }
            if (literal!=null && column!=null) {
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
     * Copy the relevant relational conditions from cur to dst.
     * The relevant ones are linked to the src reference.
     */
    private void copyRelationalConditions(MvJoinSource cur,
            MvJoinSource src, MvJoinSource dst, MvTarget result) {
        for (MvJoinCondition cond : cur.getConditions()) {
            MvJoinCondition copy = null;
            if (cond.getFirstRef()==src && cond.getSecondRef()!=null) {
                copy = new MvJoinCondition();
                copy.setFirstRef(dst);
                copy.setFirstAlias(dst.getTableAlias());
                copy.setFirstColumn(cond.getFirstColumn());
                copy.setSecondRef(result.getSourceByAlias(cond.getSecondAlias()));
                copy.setSecondAlias(cond.getSecondAlias());
                copy.setSecondColumn(cond.getSecondColumn());
            } else if (cond.getSecondRef()==src && cond.getFirstRef()!=null) {
                copy = new MvJoinCondition();
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
        if (lit1==null && lit2==null) {
            return true;
        }
        if (lit1==null || lit2==null) {
            return false;
        }
        return lit1.equals(lit2);
    }

    /**
     * Clones a MvJoinSource with a new SQL position.
     */
    private static MvJoinSource cloneJoinSource(MvJoinSource original) {
        MvJoinSource clone = new MvJoinSource();
        clone.setTableName(original.getTableName());
        clone.setTableAlias(original.getTableAlias());
        clone.setMode(original.getMode());
        clone.setTableInfo(original.getTableInfo());
        clone.setInput(original.getInput());
        return clone;
    }

    /**
     * Clones a MvJoinCondition, updating source references to point to sources
     * in the new target.
     */
    private static MvJoinCondition cloneJoinCondition(MvJoinCondition original, MvTarget newTarget) {
        MvJoinCondition clone = new MvJoinCondition();

        // Copy literals, ensuring they are added to the new target
        if (original.getFirstLiteral() != null) {
            MvLiteral newFirstLiteral = newTarget.addLiteral(original.getFirstLiteral().getValue());
            clone.setFirstLiteral(newFirstLiteral);
        }
        if (original.getSecondLiteral() != null) {
            MvLiteral newSecondLiteral = newTarget.addLiteral(original.getSecondLiteral().getValue());
            clone.setSecondLiteral(newSecondLiteral);
        }

        clone.setFirstAlias(original.getFirstAlias());
        clone.setFirstColumn(original.getFirstColumn());
        clone.setSecondAlias(original.getSecondAlias());
        clone.setSecondColumn(original.getSecondColumn());

        // Update source references to point to sources in the new target
        if (original.getFirstRef() != null) {
            String firstAlias = original.getFirstAlias();
            if (firstAlias == null && original.getFirstRef() != null) {
                firstAlias = original.getFirstRef().getTableAlias();
            }
            MvJoinSource newFirstRef = newTarget.getSourceByAlias(firstAlias);
            clone.setFirstRef(newFirstRef);
        }

        if (original.getSecondRef() != null) {
            String secondAlias = original.getSecondAlias();
            if (secondAlias == null && original.getSecondRef() != null) {
                secondAlias = original.getSecondRef().getTableAlias();
            }
            MvJoinSource newSecondRef = newTarget.getSourceByAlias(secondAlias);
            clone.setSecondRef(newSecondRef);
        }

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
