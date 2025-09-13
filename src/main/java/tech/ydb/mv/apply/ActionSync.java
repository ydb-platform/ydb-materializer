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
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.Value;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
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

    private final ThreadLocal<CompletableFuture<Result<QueryInfo>>> currentStatement
            = new ThreadLocal<>();

    public ActionSync(MvTarget target, MvActionContext context) {
        super(context);
        if (target==null || target.getSources().isEmpty()
                || target.getTopMostSource().getChangefeedInfo()==null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.targetTableName = target.getName();
        try (MvSqlGen sg = new MvSqlGen(target)) {
            this.sqlSelect = sg.makeSelect();
            this.sqlUpsert = sg.makePlainUpsert();
            this.sqlDelete = sg.makePlainDelete();
            this.rowType = sg.toRowType();
        }
        MvJoinSource src = target.getTopMostSource();
        LOG.info(" [{}] Handler `{}`, target `{}`, input `{}` as `{}`, changefeed `{}` mode {}",
                instance, context.getMetadata().getName(), target.getName(),
                src.getTableName(), src.getTableAlias(),
                src.getChangefeedInfo().getName(),
                src.getChangefeedInfo().getMode());
    }

    @Override
    public String getSqlSelect() {
        return sqlSelect;
    }

    @Override
    public StructType getRowType() {
        return rowType;
    }

    @Override
    public String toString() {
        return "MvSynchronize{" + targetTableName + '}';
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

    private void deleteRows(List<MvKey> workUpsert) {
        int writeBatchSize = getWriteBatchSize();
        for (List<MvKey> dr : Lists.partition(workUpsert, writeBatchSize)) {
            // delete some records by keys
            runDelete(dr);
        }
    }

    private void runDelete(List<MvKey> items) {
        Value<?> keys = keysToParam(items);
        if (LOG.isDebugEnabled()) {
            LOG.debug("DELETE FROM {}: {}", targetTableName, keys);
        }
        Params params = Params.of(MvSqlGen.SYS_KEYS_VAR, keys);
        // wait for the previous query to complete
        finishStatement();
        // submit the new query
        var statement = context.getRetryCtx().supplyResult(
                qs -> qs.createQuery(sqlDelete, TxMode.SERIALIZABLE_RW, params).execute());
        currentStatement.set(statement);
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
                runUpsert(wr);
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
        var statement = context.getRetryCtx().supplyResult(
                qs -> qs.createQuery(sqlUpsert, TxMode.SERIALIZABLE_RW, params).execute());
        currentStatement.set(statement);
    }

    private void finishStatement() {
        var statement = currentStatement.get();
        if (statement!=null) {
            currentStatement.remove();
            statement.join().getStatus().expectSuccess();
        }
    }

}
