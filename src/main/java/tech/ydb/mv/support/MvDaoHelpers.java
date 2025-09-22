package tech.ydb.mv.support;

import java.time.Instant;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 *
 * @author zinal
 */
public class MvDaoHelpers {

    protected static Value<?> uint64(Long value) {
        if (value == null) {
            return PrimitiveType.Uint64.makeOptional().emptyValue();
        }
        return PrimitiveValue.newUint64(value).makeOptional();
    }

    protected static Value<?> timestamp(Instant value) {
        if (value == null) {
            return PrimitiveType.Timestamp.makeOptional().emptyValue();
        }
        return PrimitiveValue.newTimestamp(value).makeOptional();
    }

    protected static Value<?> text(String value) {
        if (value == null) {
            return PrimitiveType.Text.makeOptional().emptyValue();
        }
        return PrimitiveValue.newText(value).makeOptional();
    }

    protected static Value<?> jsonDocument(String value) {
        if (value == null) {
            return PrimitiveType.JsonDocument.makeOptional().emptyValue();
        }
        return PrimitiveValue.newJsonDocument(value).makeOptional();
    }

    protected static String getText(ResultSetReader reader, String column) {
        var c = reader.getColumn(column);
        if (c.isOptionalItemPresent()) {
            return c.getText();
        }
        return null;
    }

    protected static String getJsonDocument(ResultSetReader reader, String column) {
        var c = reader.getColumn(column);
        if (c.isOptionalItemPresent()) {
            return c.getJsonDocument();
        }
        return null;
    }

}
