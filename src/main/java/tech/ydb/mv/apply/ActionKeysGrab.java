package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.table.values.StructType;
import tech.ydb.table.result.ResultSetReader;

import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbConv;

/**
 *
 * @author zinal
 */
class ActionKeysGrab extends ActionKeysAbstract {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ActionKeysGrab.class);

    private final String sqlSelect;
    private final StructType rowType;

    public ActionKeysGrab(MvTarget target, MvJoinSource src,
            MvTarget transformation, MvActionContext context) {
        super(target, src, transformation, context);
        try (MvSqlGen sg = new MvSqlGen(this.transformation)) {
            this.sqlSelect = sg.makeSelect();
            this.rowType = sg.toRowType();
        }
        LOG.info(" * Handler {}, target {}, input {} as {}, changefeed {} mode {}",
                context.getMetadata().getName(), target.getName(),
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
        return "MvKeysGrab{" + inputTableName
                + " AS " + inputTableAlias + " -> "
                + target.getName() + '}';
    }

    @Override
    protected void process(MvCommitHandler handler, List<MvApplyTask> tasks) {
        ResultSetReader rows = readRows(tasks.stream()
                .map(task -> task.getData().getKey())
                .toList());
        if (rows.getRowCount()==0) {
            return;
        }
        if (rows.getColumnCount() < keyInfo.size()) {
            throw new IllegalStateException("Actual output coluumns: "
                    + rows.getColumnCount() + ", expected: " + keyInfo.size());
        }
        // Convert the keys to change records.
        ArrayList<MvChangeRecord> output = new ArrayList<>(rows.getRowCount());
        while (rows.next()) {
            Comparable<?>[] values = new Comparable<?>[keyInfo.size()];
            for (int pos = 0; pos < keyInfo.size(); ++pos) {
                values[pos] = YdbConv.toPojo(rows.getColumn(pos).getValue());
            }
            MvKey key = new MvKey(keyInfo, values);
            output.add(new MvChangeRecord(key));
        }
        // Allow for extra operations before the actual commit.
        handler.apply(-1 * output.size());
        // Send the keys for processing.
        context.getApplyManager().submit(output, handler);
    }

}
