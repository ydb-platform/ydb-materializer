package tech.ydb.mv.apply;

import java.util.List;

import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * Single-step input key transformation to the keys of the main table for a specific MV.
 *
 * @author zinal
 */
public class MvActionTransform extends MvActionBase implements MvApplyAction {

    private final MvTarget target;
    private final MvTarget transformation;
    private final String inputTableName;
    private final String inputTableAlias;

    public MvActionTransform(MvTarget target, MvJoinSource js,
            MvTarget transformation, MvActionContext context) {
        super(context);
        this.target = target;
        this.transformation = transformation;
        this.inputTableName = js.getTableName();
        this.inputTableAlias = js.getTableAlias();
        if (!transformation.isSingleStepTransformation()) {
            throw new IllegalArgumentException();
        }

    }

    @Override
    public String toString() {
        return "MvActionTransform{" + inputTableName
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
