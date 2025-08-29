package tech.ydb.mv.apply;

import java.util.List;

import tech.ydb.table.values.StructType;

import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

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

    }

}
