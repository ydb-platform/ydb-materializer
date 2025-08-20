package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Lists;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.table.query.Params;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import tech.ydb.mv.impl.SqlGen;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbConv;

/**
 * The main action collects updates the MV for the input keys provided.
 *
 * @author zinal
 */
public class MvActionMain implements MvApplyAction {

    private final MvActionContext context;
    private final String sqlSelect;
    private final String sqlUpsert;
    private final StructType rowType;

    private final ThreadLocal<CompletableFuture<Result<QueryInfo>>> currentUpsert
            = new ThreadLocal<>();

    public MvActionMain(MvTarget target, MvActionContext context) {
        this.context = context;
        try (SqlGen sg = new SqlGen(target)) {
            this.sqlSelect = sg.makeSelect();
            this.sqlUpsert = sg.makePlainUpsert();
            this.rowType = sg.toRowType();
        }
    }

    @Override
    public void apply(List<MvApplyItem> input) {
        int readBatchSize = context.getSettings().getSelectBatchSize();
        int writeBatchSize = context.getSettings().getUpsertBatchSize();
        if (readBatchSize < 1) {
            readBatchSize = 1;
        }
        if (writeBatchSize > readBatchSize) {
            writeBatchSize = readBatchSize;
        }
        ArrayList<StructValue> output = new ArrayList<>(readBatchSize);
        for (List<MvApplyItem> rd : Lists.partition(input, readBatchSize)) {
            output.clear();
            readRows(rd, output);
            for (List<StructValue> wr : Lists.partition(output, writeBatchSize)) {
                writeRows(wr);
            }
        }
        finishWriteRows();
        commitRows(input);
    }

    private void readRows(List<MvApplyItem> items, ArrayList<StructValue> output) {
        Params params = Params.of(SqlGen.SYS_KEYS_VAR, makeKeys(items));
        ResultSetReader result = context.getRetryCtx().supplyResult(session -> QueryReader.readFrom(
                session.createQuery(sqlSelect, TxMode.ONLINE_RO, params)
        )).join().getValue().getResultSet(0);
        if (result.getRowCount()==0) {
            return;
        }
        int[] positions = new int[rowType.getMembersCount()];
        for (int ix = 0; ix < positions.length; ++ix) {
            positions[ix] = result.getColumnIndex(rowType.getMemberName(ix));
        }
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

    private Value<?> makeKeys(List<MvApplyItem> items) {
        StructValue[] values = items.stream()
                .map(item -> item.getData().convertKeyToStructValue())
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    private void writeRows(List<StructValue> items) {
        Params params = Params.of(SqlGen.SYS_KEYS_VAR, makeRows(items));
        finishWriteRows();
        var statement = context.getRetryCtx().supplyResult(
                qs -> qs.createQuery(sqlUpsert, TxMode.SERIALIZABLE_RW, params).execute());
        currentUpsert.set(statement);
    }

    private Value<?> makeRows(List<StructValue> items) {
        StructValue[] values = items.stream()
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    private void finishWriteRows() {
        var statement = currentUpsert.get();
        if (statement!=null) {
            currentUpsert.remove();
            statement.join().getStatus().expectSuccess();
        }
    }

    private void commitRows(List<MvApplyItem> input) {
        for (MvApplyItem item : input) {
            item.getCommit().apply(1);
        }
    }

}
