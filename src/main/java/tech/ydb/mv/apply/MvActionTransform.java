package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvKeyInfo;
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
    private final MvKeyInfo keyInfo;
    private final boolean keysTransform;

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
        this.keyInfo = target.getSources().get(0).getTableInfo().getKeyInfo();
        this.keysTransform = transformation.isKeyOnlyTransformation();
        if (this.keyInfo.size() != this.transformation.getColumns().size()) {
            throw new IllegalArgumentException("Illegal key setup, expected "
                    + this.keyInfo.size() + ", got " + this.keyInfo.size());
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
    public void apply(List<MvApplyTask> input) {
        new PerCommit(input).apply();
    }

    private void process(MvCommitHandler handler, List<MvApplyTask> tasks) {
        ArrayList<MvChangeRecord> output = new ArrayList<>(2 * tasks.size());
        for (MvApplyTask task : tasks) {
            MvChangeRecord cr = task.getData();
            if (keysTransform) {
                MvKey k = buildKey((name) -> cr.getKey().getValue(name));
                if (k!=null) {
                    output.add(new MvChangeRecord(k, cr.getOperationType()));
                }
            } else {
                if (cr.getImageBefore()!=null) {
                    MvKey k = buildKey((name) -> cr.getImageBefore().get(name));
                    output.add(new MvChangeRecord(k, cr.getOperationType()));
                }
                if (cr.getImageAfter()!=null) {
                    MvKey k = buildKey((name) -> cr.getImageAfter().get(name));
                    output.add(new MvChangeRecord(k, cr.getOperationType()));
                }
            }
        }
        if (! output.isEmpty()) {
            // extra records to be committed
            handler.apply(-1 * output.size());
            // submit the extracted keys for processing
            context.getApplyManager().submit(output, handler);
        }
    }

    private MvKey buildKey(Grabber grabber) {
        Comparable<?>[] values = new Comparable<?>[keyInfo.size()];
        for (int i = 0; i < keyInfo.size(); ++i) {
            MvColumn col = transformation.getColumns().get(i);
            if (col.isReference()) {
                values[i] = grabber.getValue(col.getSourceColumn());
            } else if (col.getComputation().isLiteral()) {
                values[i] = col.getComputation().getLiteral().getPojo();
            }
        }
        return new MvKey(keyInfo, values);
    }

    private static interface Grabber {
        Comparable<?> getValue(String name);
    }

    /**
     * Group the input records by commit handlers.
     * This enables more efficient per-commit-handler behavior.
     */
    private class PerCommit {
        final HashMap<MvCommitHandler, ArrayList<MvApplyTask>> items = new HashMap<>();

        PerCommit(List<MvApplyTask> tasks) {
            for (MvApplyTask task : tasks) {
                ArrayList<MvApplyTask> cur = items.get(task.getCommit());
                if (cur==null) {
                    cur = new ArrayList<>();
                    items.put(task.getCommit(), cur);
                }
                cur.add(task);
            }
        }

        void apply() {
            items.forEach((handler, tasks) -> process(handler, tasks));
        }
    }
}
