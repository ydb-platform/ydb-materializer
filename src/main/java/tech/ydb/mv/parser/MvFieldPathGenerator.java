package tech.ydb.mv.parser;

import java.util.ArrayList;
import java.util.List;

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
     * Creates a simple direct target for the case where target source
     * is the top-most source.
     */
    private MvTarget createSimpleTarget(MvJoinSource source, List<String> fieldNames) {
        MvTarget result = new MvTarget(source.getTableName() + "_simple", source.getSqlPos());
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

}
