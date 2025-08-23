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
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvLiteral;
import tech.ydb.mv.model.MvSqlPos;
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

    /**
     * Generates a minimal transformation target from the input join source to
     * the top-most join source.
     *
     * @param originalTarget The original MvTarget containing all join sources
     * @param inputSource The input MvJoinSource to transform from
     * @return A new MvTarget defining the minimal transformation, or null if no
     * path exists
     * @throws IllegalArgumentException if parameters are invalid
     */
    public static MvTarget generateKeyPath(MvTarget originalTarget, MvJoinSource inputSource) {
        if (originalTarget == null) {
            throw new IllegalArgumentException("Original target cannot be null");
        }
        if (inputSource == null) {
            throw new IllegalArgumentException("Input source cannot be null");
        }
        if (originalTarget.getSources().isEmpty()) {
            throw new IllegalArgumentException("Original target must have at least one source");
        }

        // Validate that inputSource is part of the originalTarget
        if (!originalTarget.getSources().contains(inputSource)) {
            throw new IllegalArgumentException("Input source must be part of the original target");
        }

        MvJoinSource topMostSource = originalTarget.getSources().get(0);

        // If input source is already the top-most source, create a simple target
        if (inputSource == topMostSource) {
            return createDirectTarget(inputSource);
        }

        // Check if the input source already contains all primary key fields of the top-most source
        if (canDirectlyMapKeys(inputSource, topMostSource)) {
            return createOptimizedDirectTarget(inputSource, topMostSource);
        }

        PathFinder pathFinder = new PathFinder(originalTarget);
        List<MvJoinSource> path = pathFinder.findPath(inputSource, topMostSource);

        if (path == null || path.isEmpty()) {
            return null; // No path found
        }

        return createTransformationTarget(path, originalTarget, topMostSource);
    }

    /**
     * Checks if the input source can directly map to the target primary key
     * without joins. This analyzes join conditions to find direct mappings
     * between columns or literal values.
     */
    private static boolean canDirectlyMapKeys(MvJoinSource inputSource, MvJoinSource topMostSource) {
        if (inputSource.getTableInfo() == null || topMostSource.getTableInfo() == null) {
            return false;
        }

        List<String> targetKeys = topMostSource.getTableInfo().getKey();

        // Check if we can find a direct mapping for each target key through join conditions
        for (String targetKey : targetKeys) {
            if (!canMapTargetKey(inputSource, topMostSource, targetKey)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if a specific target key can be mapped directly from the input
     * source by analyzing join conditions.
     */
    private static boolean canMapTargetKey(MvJoinSource inputSource, MvJoinSource topMostSource, String targetKey) {
        // Look through all join conditions in the input source
        for (MvJoinCondition condition : inputSource.getConditions()) {
            if (isConditionMappingKey(condition, inputSource, topMostSource, targetKey)) {
                return true;
            }
        }

        // Only allow direct column mapping if inputSource IS the topMostSource
        if (inputSource == topMostSource && inputSource.getTableInfo().getColumns().containsKey(targetKey)) {
            return true;
        }

        return false;
    }

    /**
     * Checks if a join condition provides a mapping for the target key.
     */
    private static boolean isConditionMappingKey(MvJoinCondition condition, MvJoinSource inputSource,
            MvJoinSource topMostSource, String targetKey) {

        // Check if condition connects inputSource column to topMostSource key
        if (condition.getFirstRef() == inputSource && condition.getSecondRef() == topMostSource) {
            if (targetKey.equals(condition.getSecondColumn())) {
                // inputSource.firstColumn = topMostSource.targetKey
                return true;
            }
        }

        if (condition.getFirstRef() == topMostSource && condition.getSecondRef() == inputSource) {
            if (targetKey.equals(condition.getFirstColumn())) {
                // topMostSource.targetKey = inputSource.secondColumn
                return true;
            }
        }

        // Handle alias-based references
        String inputAlias = inputSource.getTableAlias();
        String topMostAlias = topMostSource.getTableAlias();

        if (inputAlias.equals(condition.getFirstAlias()) && topMostAlias.equals(condition.getSecondAlias())) {
            if (targetKey.equals(condition.getSecondColumn())) {
                return true;
            }
        }

        if (topMostAlias.equals(condition.getFirstAlias()) && inputAlias.equals(condition.getSecondAlias())) {
            if (targetKey.equals(condition.getFirstColumn())) {
                return true;
            }
        }

        // Check for literal values (constant mappings)
        if (condition.getSecondLiteral() != null) {
            if ((condition.getFirstRef() == inputSource || inputAlias.equals(condition.getFirstAlias()))
                    && (condition.getSecondRef() == topMostSource || topMostAlias.equals(condition.getSecondAlias()))
                    && targetKey.equals(condition.getSecondColumn())) {
                return true;
            }
        }

        if (condition.getFirstLiteral() != null) {
            if ((condition.getSecondRef() == inputSource || inputAlias.equals(condition.getSecondAlias()))
                    && (condition.getFirstRef() == topMostSource || topMostAlias.equals(condition.getFirstAlias()))
                    && targetKey.equals(condition.getFirstColumn())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Creates an optimized direct target that maps keys without unnecessary
     * joins.
     */
    private static MvTarget createOptimizedDirectTarget(MvJoinSource inputSource, MvJoinSource topMostSource) {
        MvTarget result = new MvTarget("key_path_direct", new MvSqlPos(0, 0));

        // Add the input source as the main source
        MvJoinSource newSource = cloneJoinSource(inputSource);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Copy relevant join conditions from the input source that relate to the optimization
        copyOptimizationJoinConditions(inputSource, newSource, topMostSource);

        // Add columns for the target primary key fields, mapping from join conditions
        if (topMostSource.getTableInfo() != null && inputSource.getTableInfo() != null) {
            List<String> targetKeys = topMostSource.getTableInfo().getKey();

            for (String targetKey : targetKeys) {
                String sourceColumn = findSourceColumnForTargetKey(inputSource, topMostSource, targetKey);
                MvLiteral literalValue = findLiteralValueForTargetKey(inputSource, topMostSource, targetKey);

                if (sourceColumn != null) {
                    MvColumn column = new MvColumn(targetKey, new MvSqlPos(0, 0));
                    column.setSourceAlias(newSource.getTableAlias());
                    column.setSourceColumn(sourceColumn);
                    column.setSourceRef(newSource);
                    column.setType(inputSource.getTableInfo().getColumns().get(sourceColumn));
                    result.getColumns().add(column);
                } else if (literalValue != null) {
                    // Handle literal/constant values
                    MvColumn column = new MvColumn(targetKey, new MvSqlPos(0, 0));
                    column.setSourceAlias(newSource.getTableAlias());
                    column.setSourceColumn(literalValue.getValue()); // Use literal value as source
                    column.setSourceRef(newSource);
                    // Type will be determined from the target key
                    column.setType(topMostSource.getTableInfo().getColumns().get(targetKey));
                    result.getColumns().add(column);
                }
            }
        }

        return result;
    }

    /**
     * Copies join conditions that are relevant for the optimization case. For
     * optimized direct targets, we need to preserve the join conditions that
     * define the relationship between the input source and the target source.
     */
    private static void copyOptimizationJoinConditions(MvJoinSource originalInputSource,
            MvJoinSource newInputSource, MvJoinSource topMostSource) {

        // Copy join conditions from the original input source that relate to the topMostSource
        for (MvJoinCondition originalCondition : originalInputSource.getConditions()) {
            if (isConditionRelevantForOptimization(originalCondition, originalInputSource, topMostSource)) {
                MvJoinCondition newCondition = cloneJoinConditionForOptimization(
                        originalCondition, originalInputSource, newInputSource, topMostSource);
                if (newCondition != null) {
                    newInputSource.getConditions().add(newCondition);
                }
            }
        }
    }

    /**
     * Checks if a join condition is relevant for the optimization (relates to
     * the target source).
     */
    private static boolean isConditionRelevantForOptimization(MvJoinCondition condition,
            MvJoinSource inputSource, MvJoinSource topMostSource) {

        // Check if the condition connects inputSource to topMostSource
        String inputAlias = inputSource.getTableAlias();
        String topMostAlias = topMostSource.getTableAlias();

        // Direct reference check
        if ((condition.getFirstRef() == inputSource && condition.getSecondRef() == topMostSource)
                || (condition.getFirstRef() == topMostSource && condition.getSecondRef() == inputSource)) {
            return true;
        }

        // Alias-based reference check
        if ((inputAlias.equals(condition.getFirstAlias()) && topMostAlias.equals(condition.getSecondAlias()))
                || (topMostAlias.equals(condition.getFirstAlias()) && inputAlias.equals(condition.getSecondAlias()))) {
            return true;
        }

        return false;
    }

    /**
     * Clones a join condition for use in the optimized target, updating
     * references as needed.
     */
    private static MvJoinCondition cloneJoinConditionForOptimization(MvJoinCondition originalCondition,
            MvJoinSource originalInputSource, MvJoinSource newInputSource, MvJoinSource topMostSource) {

        MvJoinCondition newCondition = new MvJoinCondition(new MvSqlPos(0, 0));

        // Copy the condition, but don't include references to the topMostSource since we're not including it
        // Instead, document the condition for reference but don't actually use it for joins
        // For optimized targets, we typically don't need the actual join conditions in the generated SQL
        // since we're using the foreign key values directly. However, we preserve them for documentation.
        newCondition.setFirstLiteral(originalCondition.getFirstLiteral());
        newCondition.setFirstAlias(originalCondition.getFirstAlias());
        newCondition.setFirstColumn(originalCondition.getFirstColumn());
        newCondition.setSecondLiteral(originalCondition.getSecondLiteral());
        newCondition.setSecondAlias(originalCondition.getSecondAlias());
        newCondition.setSecondColumn(originalCondition.getSecondColumn());

        // Update source references: replace originalInputSource with newInputSource
        if (originalCondition.getFirstRef() == originalInputSource) {
            newCondition.setFirstRef(newInputSource);
        } else {
            newCondition.setFirstRef(originalCondition.getFirstRef());
        }

        if (originalCondition.getSecondRef() == originalInputSource) {
            newCondition.setSecondRef(newInputSource);
        } else {
            newCondition.setSecondRef(originalCondition.getSecondRef());
        }

        return newCondition;
    }

    /**
     * Finds the source column in inputSource that maps to the target key
     * through join conditions.
     */
    private static String findSourceColumnForTargetKey(MvJoinSource inputSource, MvJoinSource topMostSource, String targetKey) {
        // Check join conditions for column mappings
        for (MvJoinCondition condition : inputSource.getConditions()) {
            String sourceColumn = getSourceColumnFromCondition(condition, inputSource, topMostSource, targetKey);
            if (sourceColumn != null) {
                return sourceColumn;
            }
        }

        // Only allow direct column mapping if inputSource IS the topMostSource
        if (inputSource == topMostSource && inputSource.getTableInfo().getColumns().containsKey(targetKey)) {
            return targetKey;
        }

        return null;
    }

    /**
     * Finds the literal value that maps to the target key through join
     * conditions.
     */
    private static MvLiteral findLiteralValueForTargetKey(MvJoinSource inputSource, MvJoinSource topMostSource, String targetKey) {
        for (MvJoinCondition condition : inputSource.getConditions()) {
            MvLiteral literal = getLiteralFromCondition(condition, inputSource, topMostSource, targetKey);
            if (literal != null) {
                return literal;
            }
        }
        return null;
    }

    /**
     * Extracts the source column from a join condition if it maps to the target
     * key.
     */
    private static String getSourceColumnFromCondition(MvJoinCondition condition, MvJoinSource inputSource,
            MvJoinSource topMostSource, String targetKey) {

        String inputAlias = inputSource.getTableAlias();
        String topMostAlias = topMostSource.getTableAlias();

        // Check direct references: inputSource.column = topMostSource.targetKey
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

        // Check alias-based references
        if (inputAlias.equals(condition.getFirstAlias()) && topMostAlias.equals(condition.getSecondAlias())) {
            if (targetKey.equals(condition.getSecondColumn())) {
                return condition.getFirstColumn();
            }
        }

        if (topMostAlias.equals(condition.getFirstAlias()) && inputAlias.equals(condition.getSecondAlias())) {
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
    private static MvLiteral getLiteralFromCondition(MvJoinCondition condition, MvJoinSource inputSource,
            MvJoinSource topMostSource, String targetKey) {

        String topMostAlias = topMostSource.getTableAlias();

        // Check for literal mappings: topMostSource.targetKey = literal
        if (condition.getSecondLiteral() != null) {
            if ((condition.getFirstRef() == topMostSource || topMostAlias.equals(condition.getFirstAlias()))
                    && targetKey.equals(condition.getFirstColumn())) {
                return condition.getSecondLiteral();
            }
        }

        if (condition.getFirstLiteral() != null) {
            if ((condition.getSecondRef() == topMostSource || topMostAlias.equals(condition.getSecondAlias()))
                    && targetKey.equals(condition.getSecondColumn())) {
                return condition.getFirstLiteral();
            }
        }

        return null;
    }

    /**
     * Creates a direct target for the case where input source is the top-most
     * source.
     */
    private static MvTarget createDirectTarget(MvJoinSource source) {
        MvTarget result = new MvTarget("key_path_direct", new MvSqlPos(0, 0));

        // Add the source as the main source
        MvJoinSource newSource = cloneJoinSource(source);
        newSource.setMode(MvJoinMode.MAIN);
        result.getSources().add(newSource);

        // Add columns for all primary key fields
        if (source.getTableInfo() != null) {
            for (String keyColumn : source.getTableInfo().getKey()) {
                MvColumn column = new MvColumn(keyColumn, new MvSqlPos(0, 0));
                column.setSourceAlias(newSource.getTableAlias());
                column.setSourceColumn(keyColumn);
                column.setSourceRef(newSource);
                column.setType(source.getTableInfo().getColumns().get(keyColumn));
                result.getColumns().add(column);
            }
        }

        return result;
    }

    /**
     * Creates a transformation target based on the found path.
     */
    private static MvTarget createTransformationTarget(List<MvJoinSource> path,
            MvTarget originalTarget, MvJoinSource topMostSource) {
        MvTarget result = new MvTarget("key_path_transform", new MvSqlPos(0, 0));

        // Add sources in the path
        for (int i = 0; i < path.size(); i++) {
            MvJoinSource originalSource = path.get(i);
            MvJoinSource newSource = cloneJoinSource(originalSource);

            if (i == 0) {
                newSource.setMode(MvJoinMode.MAIN);
            } else {
                // Inner join, because we assume that the path exists
                newSource.setMode(MvJoinMode.INNER);
                // Copy relevant join conditions
                copyRelevantConditions(originalSource, newSource, path, i, originalTarget, result);
            }

            result.getSources().add(newSource);
        }

        // Add columns for the target primary key (top-most source key)
        if (topMostSource.getTableInfo() != null) {
            MvJoinSource targetSourceInResult = result.getSources().get(result.getSources().size() - 1);
            for (String keyColumn : topMostSource.getTableInfo().getKey()) {
                MvColumn column = new MvColumn(keyColumn, new MvSqlPos(0, 0));
                column.setSourceAlias(targetSourceInResult.getTableAlias());
                column.setSourceColumn(keyColumn);
                column.setSourceRef(targetSourceInResult);
                column.setType(topMostSource.getTableInfo().getColumns().get(keyColumn));
                result.getColumns().add(column);
            }
        }

        return result;
    }

    /**
     * Copies join conditions that are relevant for the current step in the
     * path.
     */
    private static void copyRelevantConditions(MvJoinSource originalSource, MvJoinSource newSource,
            List<MvJoinSource> path, int currentIndex, MvTarget originalTarget, MvTarget newTarget) {

        MvJoinSource previousInPath = path.get(currentIndex - 1);

        for (MvJoinCondition condition : originalSource.getConditions()) {
            // Check if this condition connects to the previous source in our path
            if (isConditionRelevantForPath(condition, originalSource, previousInPath, originalTarget)) {
                MvJoinCondition newCondition = cloneJoinCondition(condition, newTarget);
                newSource.getConditions().add(newCondition);
            }
        }
    }

    /**
     * Checks if a join condition is relevant for connecting two sources in the
     * path.
     */
    private static boolean isConditionRelevantForPath(MvJoinCondition condition,
            MvJoinSource currentSource, MvJoinSource previousSource, MvTarget originalTarget) {

        // Check if the condition references the previous source in the path
        MvJoinSource firstRef = condition.getFirstRef();
        MvJoinSource secondRef = condition.getSecondRef();

        // Handle alias-based references
        if (firstRef == null && condition.getFirstAlias() != null) {
            firstRef = originalTarget.getSourceByAlias(condition.getFirstAlias());
        }
        if (secondRef == null && condition.getSecondAlias() != null) {
            secondRef = originalTarget.getSourceByAlias(condition.getSecondAlias());
        }

        return (firstRef == previousSource && secondRef == currentSource)
                || (firstRef == currentSource && secondRef == previousSource);
    }

    /**
     * Clones a MvJoinSource with a new SQL position.
     */
    private static MvJoinSource cloneJoinSource(MvJoinSource original) {
        MvJoinSource clone = new MvJoinSource(new MvSqlPos(0, 0));
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
        MvJoinCondition clone = new MvJoinCondition(new MvSqlPos(0, 0));
        clone.setFirstLiteral(original.getFirstLiteral());
        clone.setFirstAlias(original.getFirstAlias());
        clone.setFirstColumn(original.getFirstColumn());
        clone.setSecondLiteral(original.getSecondLiteral());
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
     * Inner class to find the path between two join sources.
     */
    private static class PathFinder {

        private final MvTarget target;
        private final Map<MvJoinSource, List<MvJoinSource>> adjacencyMap;

        public PathFinder(MvTarget target) {
            this.target = target;
            this.adjacencyMap = buildAdjacencyMap();
        }

        /**
         * Builds an adjacency map representing the join relationships.
         */
        private Map<MvJoinSource, List<MvJoinSource>> buildAdjacencyMap() {
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
        public List<MvJoinSource> findPath(MvJoinSource from, MvJoinSource to) {
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
            return path;
        }
    }
}
