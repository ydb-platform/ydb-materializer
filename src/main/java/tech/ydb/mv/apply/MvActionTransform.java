package tech.ydb.mv.apply;

import java.util.List;

import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * Single-step input key transformation to the keys of the main table for a specific MV.
 *
 * @author zinal
 */
public class MvActionTransform extends MvActionBase implements MvApplyAction {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvActionTransform.class);

    private final MvTarget target;
    private final MvTarget transformation;
    private final String inputTableName;
    private final String inputTableAlias;

    public MvActionTransform(MvTarget target, MvJoinSource src,
            MvTarget transformation, MvActionContext context) {
        super(context);
        if (target==null || src==null || src.getChangefeedInfo()==null
                || transformation==null) {
            throw new IllegalArgumentException("Missing input");
        }
        this.target = target;
        this.transformation = transformation;
        this.inputTableName = src.getTableName();
        this.inputTableAlias = src.getTableAlias();
        if (!transformation.isSingleStepTransformation()) {
            throw new IllegalArgumentException("Single step transformation should be passed");
        }
        LOG.info(" * Handler {}, target {}, input {} as {}, changefeed {} mode {}",
                context.getMetadata().getName(), target.getName(),
                src.getTableName(), src.getTableAlias(),
                src.getChangefeedInfo().getName(),
                src.getChangefeedInfo().getMode());
    }

    @Override
    public String toString() {
        return "MvActionTransform{" + inputTableName
                + " AS " + inputTableAlias + " -> "
                + target.getName() + '}';
    }

    @Override
    public void apply(List<MvChangeRecord> input) {
        if (input==null || input.isEmpty()) {
            return;
        }
    }

}
