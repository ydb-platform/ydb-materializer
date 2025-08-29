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
public class MvActionGrabKeys extends MvActionBase implements MvApplyAction {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvActionGrabKeys.class);

    private final MvTarget target;
    private final MvTarget transformation;
    private final String inputTableName;
    private final String inputTableAlias;
    private final String sqlSelect;
    private final StructType rowType;

    public MvActionGrabKeys(MvTarget target, MvJoinSource src,
            MvTarget transformation, MvActionContext context) {
        super(context);
        if (target==null || src==null || transformation==null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.target = target;
        this.transformation = transformation;
        this.inputTableName = src.getTableName();
        this.inputTableAlias = src.getTableAlias();
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
        return "MvActionGrabKeys{" + inputTableName
                + " AS " + inputTableAlias + " -> "
                + target.getName() + '}';
    }

    @Override
    public void apply(List<MvApplyTask> input) {
        if (input==null || input.isEmpty()) {
            return;
        }
    }

}
