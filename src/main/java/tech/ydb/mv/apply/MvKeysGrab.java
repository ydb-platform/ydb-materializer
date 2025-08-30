package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.table.values.StructType;

import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbConv;
import tech.ydb.table.values.StructValue;

/**
 *
 * @author zinal
 */
public class MvKeysGrab extends MvKeysAbstract implements MvApplyAction {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvKeysGrab.class);

    private final String sqlSelect;
    private final StructType rowType;

    public MvKeysGrab(MvTarget target, MvJoinSource src,
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
        List<MvKey> inputKeys = tasks.stream()
                .map(task -> task.getData().getKey())
                .toList();
        ArrayList<StructValue> outputRows = new ArrayList<>();
        readRows(inputKeys, outputRows);
        ArrayList<MvKey> outputKeys = new ArrayList<>(outputRows.size());
        for (StructValue sv : outputRows) {
            Comparable<?>[] values = new Comparable<?>[keyInfo.size()];
            for (int pos = 0; pos < keyInfo.size(); ++pos) {
                // TODO
            }
            outputKeys.add(new MvKey(keyInfo, values));
        }
    }

}
