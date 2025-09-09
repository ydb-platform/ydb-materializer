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
            return createDirectTarget(point, topMostTable.getKey(), true);
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
        MvTarget result = new MvTarget(source.getTableName() + "_simple", source.getSqlPos());
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
     * Creates a transformation target based on the found path.
     */
    private MvTarget createTarget(List<MvJoinSource> path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        return MvGeneratorBase.createTarget(path, path.get(0), topMostTable.getKey());
    }

}
