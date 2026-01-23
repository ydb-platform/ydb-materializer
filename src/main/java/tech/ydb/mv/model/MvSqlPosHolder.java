package tech.ydb.mv.model;

/**
 * Marker interface for objects that can provide a position in the source SQL text.
 * @author zinal
 */
public interface MvSqlPosHolder {

    /**
     * Get SQL position.
     *
     * @return SQL position associated with this object.
     */
    MvSqlPos getSqlPos();

}
