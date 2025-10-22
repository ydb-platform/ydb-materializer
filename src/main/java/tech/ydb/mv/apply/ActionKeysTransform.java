package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.List;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * Single-step input key transformation to the keys of the main table for a
 * specific MV.
 *
 * @author zinal
 */
class ActionKeysTransform extends ActionKeysAbstract {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ActionKeysTransform.class);

    private final boolean keysTransform;
    private final List<MvColumn> columns;

    public ActionKeysTransform(MvTarget target, MvJoinSource src,
            MvTarget transformation, MvActionContext context) {
        super(target, src, transformation, context);
        if (!transformation.isSingleStepTransformation()) {
            throw new IllegalArgumentException("Single step transformation should be passed");
        }
        if (this.keyInfo.size() != transformation.getColumns().size()) {
            throw new IllegalArgumentException("Illegal key setup, expected "
                    + this.keyInfo.size() + ", got " + this.keyInfo.size());
        }
        this.keysTransform = transformation.isKeyOnlyTransformation();
        this.columns = transformation.getColumns();
        LOG.info(" [{}] Handler `{}`, target `{}`, input `{}` as `{}`, changefeed `{}` mode {}",
                instance, context.getMetadata().getName(), target.getViewName(),
                src.getTableName(), src.getTableAlias(),
                src.getChangefeedInfo().getName(),
                src.getChangefeedInfo().getMode());
    }

    @Override
    public String toString() {
        return "MvKeysTransform{" + inputTableName
                + " AS " + inputTableAlias + " -> "
                + target.getViewName() + '}';
    }

    @Override
    protected void process(MvCommitHandler handler, List<MvApplyTask> tasks) {
        ArrayList<MvChangeRecord> output = new ArrayList<>(2 * tasks.size());
        for (MvApplyTask task : tasks) {
            MvChangeRecord cr = task.getData();
            LOG.debug("Processing {}", cr);
            MvKey key = null;
            if (keysTransform) {
                key = buildKey(cr, (name) -> cr.getKey().getValue(name));
            } else {
                if (cr.getImageBefore() != null && cr.getImageBefore().isFilled()) {
                    key = buildKey(cr, (name) -> cr.getImageBefore().get(name));
                }
                if (cr.getImageAfter() != null && cr.getImageAfter().isFilled()) {
                    key = buildKey(cr, (name) -> cr.getImageAfter().get(name));
                }
            }
            if (key != null) {
                LOG.debug("Result key: {}", key);
                output.add(new MvChangeRecord(key,
                        task.getData().getTv(), MvChangeRecord.OpType.UPSERT));
            }
        }
        if (!output.isEmpty()) {
            // extra records to be committed
            handler.reserve(output.size());
            // submit the extracted keys for processing
            applyManager.submitForce(null, output, handler);
        }
    }

    private MvKey buildKey(MvChangeRecord cr, Grabber grabber) {
        Comparable<?>[] values = new Comparable<?>[keyInfo.size()];
        for (int i = 0; i < keyInfo.size(); ++i) {
            MvColumn col = columns.get(i);
            String column = null;
            if (col.isReference()) {
                column = col.getSourceColumn();
                values[i] = grabber.getValue(column);
            } else if (col.getComputation().isLiteral()) {
                column = col.getComputation().getLiteral().getValue();
                values[i] = col.getComputation().getLiteral().getPojo();
            }
            if (values[i] == null) {
                LOG.warn(" [{}] Missing value for column {}, source {}", instance, column, cr);
                return null;
            }
        }
        return new MvKey(keyInfo, values);
    }

    private static interface Grabber {

        Comparable<?> getValue(String name);
    }
}
