package tech.ydb.mv.parser;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinSource;
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
public class MvFieldPathGenerator extends MvGeneratorBase {

    public MvFieldPathGenerator(MvTarget target) {
        super(target);
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
            return createSimpleTarget(targetSource, fieldNames);
        }

        // Check if we can directly map the fields without joins
        if (canDirectlyMapFields(targetSource, fieldNames)) {
            return createDirectTarget(targetSource, fieldNames, false);
        }

        // Find path from top-most source to target source
        List<MvJoinSource> path = findPath(topMostSource, targetSource);
        if (path == null || path.isEmpty()) {
            return null; // No path found
        }

        return createTarget(path, targetSource, fieldNames);
    }

    /**
     * Generates a transformation target to obtain all fields from a target table,
     * given the primary key of the top-most table.
     *
     * @param targetTableAlias The alias of the table to retrieve fields from
     * @return A new MvTarget defining the minimal transformation, or null if no path exists
     * @throws IllegalArgumentException if parameters are invalid
     */
    public MvTarget generate(String targetTableAlias) {
        MvJoinSource targetSource = originalTarget.getSourceByAlias(targetTableAlias);
        if (targetSource == null) {
            throw new IllegalArgumentException("Target table alias '" + targetTableAlias + "' not found in original target");
        }

        MvTableInfo tableInfo = targetSource.getTableInfo();
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table info not available for target table '" + targetTableAlias + "'");
        }

        List<String> allFields = new ArrayList<>(tableInfo.getColumns().keySet());
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
     * by analyzing join conditions. Used by field path generator.
     */
    private boolean canMapTargetField(MvJoinSource source, String fieldName) {
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

}
