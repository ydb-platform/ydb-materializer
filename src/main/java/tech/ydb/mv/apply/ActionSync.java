package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Lists;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.table.query.Params;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.YdbConv;
import tech.ydb.mv.metrics.MvMetrics;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvSqlGen;

/**
 * The main action collects updates the MV for the input keys provided.
 *
 * @author zinal
 */
class ActionSync extends ActionBase implements MvApplyAction {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ActionSync.class);

    private final String targetTableName;
    private final String sqlSelect;
    private final String sqlUpsert;
    private final String sqlDelete;
    private final StructType rowType;
    private final SessionRetryContext targetCtx;

    private final ThreadLocal<StatementTiming> currentStatement = new ThreadLocal<>();

    public ActionSync(MvViewExpr target, MvActionContext context) {
        super(context, MvMetrics.scopeForActionSync(context.getHandler(), target));
        if (target == null || target.getSources().isEmpty()
                || target.getTopMostSource().getChangefeedInfo() == null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.targetTableName = target.getName();
        try (MvSqlGen sg = new MvSqlGen(target)) {
            this.sqlSelect = sg.makeSelect();
            this.sqlUpsert = sg.makePlainUpsert();
            this.sqlDelete = sg.makePlainDelete();
            if (target.getTableInfo() != null) {
                this.rowType = MvSqlGen.toRowType(target.getTableInfo());
            } else {
                this.rowType = sg.toRowType();
            }
        }
        this.targetCtx = context.getJobContext().getYdb().getQueryRetryCtx(); // FIXME
        MvJoinSource src = target.getTopMostSource();
        LOG.info(" [{}] Handler `{}`, target `{}` as {}, input `{}` as `{}`, changefeed `{}` mode {}",
                instance, context.getHandler().getName(),
                target.getName(), target.getAlias(),
                src.getTableName(), src.getTableAlias(),
                src.getChangefeedInfo().getName(),
                src.getChangefeedInfo().getMode());
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    @Override
    public String getSqlSelect() {
        return sqlSelect;
    }

    @Override
    public String toString() {
        return "MvSynchronize{" + targetTableName + '}';
    }

    @Override
    public void apply(List<MvApplyTask> input) {
        if (input == null || input.isEmpty()) {
            return;
        }
        // exclude duplicate keys before the db query
        ArrayList<MvKey> workUpsert = new ArrayList<>();
        ArrayList<MvKey> workDelete = new ArrayList<>();
        deduplicate(input, workUpsert, workDelete);
        deleteRows(workDelete);
        upsertRows(workUpsert);
        // wait for the last write to be completed
        finishStatement();
    }

    private void deduplicate(List<MvApplyTask> input,
            List<MvKey> upsert, List<MvKey> delete) {
        HashSet<MvKey> tempUpsert = new HashSet<>();
        HashSet<MvKey> tempDelete = new HashSet<>();
        for (MvApplyTask task : input) {
            MvChangeRecord cr = task.getData();
            switch (cr.getOperationType()) {
                case UPSERT:
                    tempUpsert.add(cr.getKey());
                    break;
                case DELETE:
                    tempDelete.add(cr.getKey());
                    break;
            }
        }
        upsert.addAll(tempUpsert);
        delete.addAll(tempDelete);
    }

    private void deleteRows(List<MvKey> rowKeys) {
        int writeBatchSize = getWriteBatchSize();
        for (List<MvKey> dr : Lists.partition(rowKeys, writeBatchSize)) {
            // delete some records by keys
            runDelete(dr);
            // check whether the context is running, and throw if not
            checkRunning();
        }
    }

    private void runDelete(List<MvKey> rowKeys) {
        Value<?> keys = keysToParam(rowKeys);
        if (LOG.isDebugEnabled()) {
            LOG.debug("DELETE FROM {}: {}", targetTableName, keys);
        }
        Params params = Params.of(MvSqlGen.SYS_KEYS_VAR, keys);
        // wait for the previous query to complete
        finishStatement();
        // submit the new query
        lastSqlStatement.set(sqlDelete);
        long startNs = System.nanoTime();
        var statement = targetCtx.supplyResult(
                qs -> qs.createQuery(sqlDelete, TxMode.SERIALIZABLE_RW, params, querySettings)
                        .execute()
        );
        currentStatement.set(new StatementTiming(statement, startNs, "delete"));
    }

    private void upsertRows(List<MvKey> rowKeys) {
        int readBatchSize = getReadBatchSize();
        int writeBatchSize = getWriteBatchSize();
        ArrayList<StructValue> output = new ArrayList<>(readBatchSize);
        for (List<MvKey> rd : Lists.partition(rowKeys, readBatchSize)) {
            // read the portion of data
            output.clear();
            readRows(rd, output);
            for (List<StructValue> wr : Lists.partition(output, writeBatchSize)) {
                // write the portion of data
                runUpsert(wr);
                // check whether the context is running, and throw if not
                checkRunning();
            }
        }
    }

    private void runUpsert(List<StructValue> items) {
        Value<?> data = structsToParam(items);
        if (LOG.isDebugEnabled()) {
            LOG.debug("UPSERT TO {}: {}", targetTableName, data);
        }
        Params params = Params.of(MvSqlGen.SYS_INPUT_VAR, data);
        // wait for the previous query to complete
        finishStatement();
        // submit the new query
        lastSqlStatement.set(sqlUpsert);
        long startNs = System.nanoTime();
        var statement = targetCtx.supplyResult(
                qs -> qs.createQuery(sqlUpsert, TxMode.SERIALIZABLE_RW, params, querySettings)
                        .execute()
        );
        currentStatement.set(new StatementTiming(statement, startNs, "upsert"));
    }

    private void finishStatement() {
        var timing = currentStatement.get();
        if (timing != null) {
            currentStatement.remove();
            timing.future.join().getStatus().expectSuccess();
            var scope = getMetricsScope();
            if (scope != null && scope.target() != null) {
                MvMetrics.recordSqlTime(scope, timing.operation, timing.startNs);
            }
        }
        lastSqlStatement.set(null);
    }

    private void readRows(List<MvKey> items, ArrayList<StructValue> output) {
        // perform the db query
        ResultSetReader result = readRows(items);
        if (result.getRowCount() == 0) {
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
                    members[ix] = ((OptionalType) type).emptyValue();
                } else {
                    members[ix] = YdbConv.convert(result.getColumn(pos).getValue(), type);
                }
            }
            output.add(rowType.newValueUnsafe(members));
        }
    }

    private static class StatementTiming {

        final CompletableFuture<Result<QueryInfo>> future;
        final long startNs;
        final String operation;

        StatementTiming(CompletableFuture<Result<QueryInfo>> future, long startNs, String operation) {
            this.future = future;
            this.startNs = startNs;
            this.operation = operation;
        }
    }
}
