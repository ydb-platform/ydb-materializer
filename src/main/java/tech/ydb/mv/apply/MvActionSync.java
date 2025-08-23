package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Lists;
import java.util.Objects;
import java.util.UUID;

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

import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbConv;

/**
 * The main action collects updates the MV for the input keys provided.
 *
 * @author zinal
 */
public class MvActionSync implements MvApplyAction {

    private final String id;
    private final String targetTableName;
    private final MvActionContext context;
    private final String sqlSelect;
    private final String sqlUpsert;
    private final StructType rowType;

    private final ThreadLocal<CompletableFuture<Result<QueryInfo>>> currentStatement
            = new ThreadLocal<>();

    public MvActionSync(MvTarget target, MvActionContext context) {
        this.id = UUID.randomUUID().toString();
        this.context = context;
        this.targetTableName = target.getName();
        try (MvSqlGen sg = new MvSqlGen(target)) {
            this.sqlSelect = sg.makeSelect();
            this.sqlUpsert = sg.makePlainUpsert();
            this.rowType = sg.toRowType();
        }
    }

    @Override
    public String toString() {
        return "MvActionSync{" + targetTableName + '}';
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
        final MvActionSync other = (MvActionSync) obj;
        return Objects.equals(this.id, other.id);
    }

    public int getReadBatchSize() {
        int readBatchSize = context.getSettings().getSelectBatchSize();
        if (readBatchSize < 1) {
            readBatchSize = 1;
        }
        return readBatchSize;
    }

    public int getWriteBatchSize() {
        int readBatchSize = getReadBatchSize();
        int writeBatchSize = context.getSettings().getUpsertBatchSize();
        if (writeBatchSize > readBatchSize) {
            writeBatchSize = readBatchSize;
        }
        return writeBatchSize;
    }

    @Override
    public void apply(List<MvApplyTask> input) {
        if (input==null || input.isEmpty()) {
            return;
        }
        // exclude duplicate keys before the db query
        ArrayList<MvKey> workUpsert = new ArrayList<>();
        ArrayList<MvKey> workDelete = new ArrayList<>();
        deduplicate(input, workUpsert, workDelete);
        deleteRows(workDelete);
        upsertRows(workUpsert);
    }

    private void deduplicate(List<MvApplyTask> input,
            List<MvKey> upsert, List<MvKey> delete) {
        HashSet<MvKey> tempUpsert = new HashSet<>();
        HashSet<MvKey> tempDelete = new HashSet<>();
        for (MvApplyTask item : input) {
            switch (item.getData().getOperationType()) {
                case UPSERT:
                    tempUpsert.add(item.getData().getKey());
                    break;
                case DELETE:
                    tempDelete.add(item.getData().getKey());
                    break;
            }
        }
        upsert.addAll(tempUpsert);
        delete.addAll(tempDelete);
    }

    private void deleteRows(List<MvKey> workUpsert) {
        // TODO
    }

    private void upsertRows(List<MvKey> workUpsert) {
        int readBatchSize = getReadBatchSize();
        int writeBatchSize = getWriteBatchSize();
        ArrayList<StructValue> output = new ArrayList<>(readBatchSize);
        for (List<MvKey> rd : Lists.partition(workUpsert, readBatchSize)) {
            // read the portion of data
            output.clear();
            readRows(rd, output);
            for (List<StructValue> wr : Lists.partition(output, writeBatchSize)) {
                // write the portion of data
                writeRows(wr);
            }
        }
        // wait for the last write to be completed
        finishStatement();
    }

    private void readRows(List<MvKey> items, ArrayList<StructValue> output) {
        // perform the db query
        Params params = Params.of(MvSqlGen.SYS_KEYS_VAR, makeKeys(items));
        ResultSetReader result = context.getRetryCtx().supplyResult(session -> QueryReader.readFrom(
                session.createQuery(sqlSelect, TxMode.ONLINE_RO, params)
        )).join().getValue().getResultSet(0);
        if (result.getRowCount()==0) {
            return;
        }
        // map the positions of columns
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

    private Value<?> makeKeys(List<MvKey> items) {
        StructValue[] values = items.stream()
                .map(item -> item.convertKeyToStructValue())
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    private void writeRows(List<StructValue> items) {
        Params params = Params.of(MvSqlGen.SYS_KEYS_VAR, makeRows(items));
        // wait for the previous query to complete
        finishStatement();
        // submit the new query
        var statement = context.getRetryCtx().supplyResult(
                qs -> qs.createQuery(sqlUpsert, TxMode.SERIALIZABLE_RW, params).execute());
        currentStatement.set(statement);
    }

    private Value<?> makeRows(List<StructValue> items) {
        StructValue[] values = items.stream()
                .toArray(StructValue[]::new);
        return ListValue.of(values);
    }

    private void finishStatement() {
        var statement = currentStatement.get();
        if (statement!=null) {
            currentStatement.remove();
            statement.join().getStatus().expectSuccess();
        }
    }

}
