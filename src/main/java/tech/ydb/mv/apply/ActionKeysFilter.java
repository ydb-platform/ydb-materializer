package tech.ydb.mv.apply;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.MvRowFilter;
import tech.ydb.mv.data.YdbConv;
import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvSqlGen;

/**
 * Keys filter is used to skip the full refresh of unchanged records during the
 * dictionary-initiated scan.
 *
 * @author zinal
 */
class ActionKeysFilter extends ActionBase implements MvApplyAction {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ActionKeysFilter.class);

    private final MvViewExpr target;
    private final MvKeyInfo topmostKey;
    private final MvRowFilter filter;
    private final String sqlSelect;

    public ActionKeysFilter(MvActionContext context, MvViewExpr target,
            MvViewExpr request, MvRowFilter filter) {
        super(context);
        this.target = target;
        this.topmostKey = target.getTopMostSource().getTableInfo().getKeyInfo();
        this.filter = filter;
        try (MvSqlGen sg = new MvSqlGen(request)) {
            this.sqlSelect = sg.makeSelect();
        }
        var source = target.getTopMostSource();
        if (source != null) {
            setMetricsScope(target.getName(), source.getTableName(), source.getTableAlias());
        }
        LOG.info(" [{}] Handler `{}`, target `{}` as {}, total {} filter(s)",
                instance, context.getMetadata().getName(), target.getName(),
                target.getAlias(), filter.getBlocks().size());
    }

    @Override
    protected String getSqlSelect() {
        return sqlSelect;
    }

    @Override
    public void apply(List<MvApplyTask> input) {
        new PerCommit(input).apply((handler, tasks) -> process(handler, tasks));
    }

    private void process(MvCommitHandler handler, List<MvApplyTask> tasks) {
        var rsr = readTaskRows(tasks);
        var records = new ArrayList<MvChangeRecord>();
        Instant tv = Instant.now();
        while (rsr.next()) {
            var row = YdbConv.toPojoRow(rsr);
            if (filter.matches(row)) {
                records.add(convert(row, tv));
            }
        }
        if (!records.isEmpty()) {
            handler.reserve(records.size());
            applyManager.submitForce(target, records, handler);
        }
    }

    private MvChangeRecord convert(Comparable<?>[] row, Instant tv) {
        Comparable<?>[] keyPart = new Comparable<?>[topmostKey.size()];
        for (int i = 0; i < keyPart.length; ++i) {
            keyPart[i] = row[i];
        }
        MvKey key = new MvKey(topmostKey, keyPart);
        return new MvChangeRecord(key, tv, MvChangeRecord.OpType.UPSERT);
    }

}
