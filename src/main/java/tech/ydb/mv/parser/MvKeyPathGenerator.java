package tech.ydb.mv.parser;

import java.util.List;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvLiteral;
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
public class MvKeyPathGenerator extends MvGeneratorBase {

    public MvKeyPathGenerator(MvTarget target) {
        super(target);
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
     * Creates a simple direct target for the case where input source
     * is the top-most source.
     */
    private MvTarget createSimpleTarget(MvJoinSource source) {
        MvTarget result = new MvTarget(source.getTableName() + "_keys_simple", source.getSqlPos());
        result.setTableInfo(source.getTableInfo());

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
        result.setTableInfo(source.getTableInfo());

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
                MvLiteral literalValue = findLiteral(source, targetKey);
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
     * Creates a transformation target based on the found path.
     */
    private MvTarget createTarget(List<MvJoinSource> path) {
        MvTarget result = new MvTarget(path.get(0).getTableName() + "_keys_full");
        result.setTableInfo(path.get(0).getTableInfo());

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

        // Add columns for the source primary key (target bottom-most table)
        MvJoinSource targetBottom = result.getSources().get(result.getSources().size() - 1);
        for (String keyColumn : topMostTable.getKey()) {
            MvColumn column = new MvColumn(keyColumn);
            column.setSourceAlias(targetBottom.getTableAlias());
            column.setSourceColumn(keyColumn);
            column.setSourceRef(targetBottom);
            column.setType(topMostTable.getColumns().get(keyColumn));
            result.getColumns().add(column);
        }

        return result;
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

}
