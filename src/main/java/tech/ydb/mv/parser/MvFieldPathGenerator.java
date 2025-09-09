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

}
