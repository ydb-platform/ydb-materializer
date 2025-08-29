package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.util.YdbConv;

/**
 * Common parts of action handlers in the form of a base class.
 *
 * @author zinal
 */
public abstract class MvActionBase {

    protected final String id;
    protected final MvActionContext context;

    protected MvActionBase(MvActionContext context) {
        this.id = UUID.randomUUID().toString();
        this.context = context;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvActionBase other = (MvActionBase) obj;
        return Objects.equals(this.id, other.id);
    }

    protected String getSqlSelect() {
        throw new UnsupportedOperationException();
    }

    protected StructType getRowType() {
        throw new UnsupportedOperationException();
    }

    protected final void readRows(List<MvKey> items, ArrayList<StructValue> output) {
        // perform the db query
        Params params = Params.of(MvSqlGen.SYS_KEYS_VAR, keysToParam(items));
        ResultSetReader result = context.getRetryCtx().supplyResult(session -> QueryReader.readFrom(
                session.createQuery(getSqlSelect(), TxMode.ONLINE_RO, params)
        )).join().getValue().getResultSet(0);
        if (result.getRowCount()==0) {
            return;
        }
        // map the positions of columns
        final StructType rowType = getRowType();
        int[] positions = new int[rowType.getMembersCount()];
        for (int ix = 0; ix < positions.length; ++ix) {
            positions[ix] = result.getColumnIndex(rowType.getMemberName(ix));
        }
        // convert the output to the desired structures
        while (result.next()) {
            Value<?>[] members = new Value<?>[positions.length];
            for (int ix = 0; ix < positions.length; ++ix) {
                Type type = rowType.getMemberType(ix);
                int pos = positions[ix];
                if (pos < 0) {
                    members[ix] = ((OptionalType)type).emptyValue();
                } else {
                    members[ix] = YdbConv.convert(result.getColumn(pos).getValue(), type);
                }
            }
            output.add(rowType.newValueUnsafe(members));
        }
    }

    protected static Value<?> keysToParam(List<MvKey> items) {
        StructValue[] values = items.stream()
                .map(item -> item.convertKeyToStructValue())
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    protected static Value<?> structsToParam(List<StructValue> items) {
        StructValue[] values = items.stream()
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    protected final int getReadBatchSize() {
        int readBatchSize = context.getSettings().getSelectBatchSize();
        if (readBatchSize < 1) {
            readBatchSize = 1;
        }
        return readBatchSize;
    }

    protected final int getWriteBatchSize() {
        int readBatchSize = getReadBatchSize();
        int writeBatchSize = context.getSettings().getUpsertBatchSize();
        if (writeBatchSize > readBatchSize) {
            writeBatchSize = readBatchSize;
        }
        return writeBatchSize;
    }

}
