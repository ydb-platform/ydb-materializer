package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;
import tech.ydb.mv.MvConfig;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.parser.MvPathGenerator;

/**
 * The apply configuration includes the settings to process the change records
 * coming from the single input table.
 *
 * @author zinal
 */
class MvApply {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApply.class);

    static SourceBuilder newSource(MvTableInfo table, MvWorkerSelector selector) {
        return new SourceBuilder(table, selector);
    }

    static TargetBuilder newTarget(MvTarget target, MvTarget dictTrans) {
        return new TargetBuilder(target, dictTrans);
    }

    static class Source {

        private final MvTableInfo tableInfo;
        private final MvWorkerSelector selector;
        private final MvApplyActionList actions;

        Source(SourceBuilder builder) {
            this.tableInfo = builder.table;
            this.selector = builder.selector;
            this.actions = new MvApplyActionList(builder.actions);
        }

        public MvTableInfo getTableInfo() {
            return tableInfo;
        }

        public MvWorkerSelector getSelector() {
            return selector;
        }

        public MvApplyActionList getActions() {
            return actions;
        }
    }

    static class SourceBuilder {

        private final MvTableInfo table;
        private final MvWorkerSelector selector;
        private final ArrayList<MvApplyAction> actions = new ArrayList<>();

        SourceBuilder(MvTableInfo table, MvWorkerSelector selector) {
            this.table = table;
            this.selector = selector;
        }

        SourceBuilder addAction(MvApplyAction action) {
            actions.add(action);
            return this;
        }

        Source build() {
            return new Source(this);
        }
    }

    static class Target {

        private final MvTarget target;
        private final MvApplyActionList refreshActions;
        private final MvTarget dictTrans;

        Target(TargetBuilder builder) {
            this.target = builder.target;
            this.refreshActions = new MvApplyActionList(builder.actions);
            this.dictTrans = builder.dictTrans;
        }

        public MvTarget getTarget() {
            return target;
        }

        public MvApplyActionList getRefreshActions() {
            return refreshActions;
        }

        public MvTarget getDictTrans() {
            return dictTrans;
        }
    }

    static class TargetBuilder {

        private final MvTarget target;
        private final ArrayList<MvApplyAction> actions = new ArrayList<>();
        private final MvTarget dictTrans;

        TargetBuilder(MvTarget target, MvTarget dictTrans) {
            this.target = target;
            this.dictTrans = dictTrans;
        }

        TargetBuilder addAction(MvApplyAction action) {
            actions.add(action);
            return this;
        }

        Target build() {
            return new Target(this);
        }
    }

    static class Configurator {

        final MvActionContext context;
        final MvHandler metadata;
        final int workersCount;
        final MvConfig.PartitioningStrategy partitioning;
        final HashMap<String, SourceBuilder> sources = new HashMap<>();
        final HashMap<String, TargetBuilder> targets = new HashMap<>();

        public Configurator(MvActionContext context) {
            this.context = context;
            this.metadata = context.getMetadata();
            this.workersCount = context.getSettings().getApplyThreads();
            this.partitioning = context.getPartitioning();
        }

        void build(HashMap<String, Source> src, HashMap<String, Target> trg) {
            prepare();
            sources.forEach((k, v) -> src.put(k, v.build()));
            targets.forEach((k, v) -> trg.put(k, v.build()));
        }

        SourceBuilder makeSource(MvTableInfo ti) {
            var b = sources.get(ti.getName());
            if (b == null) {
                var selector = new MvWorkerSelector(ti, workersCount, partitioning);
                b = newSource(ti, selector);
                sources.put(ti.getName(), b);
            }
            return b;
        }

        TargetBuilder makeTarget(MvTarget target, MvTarget dictTrans) {
            var b = targets.get(target.getName());
            if (b == null) {
                b = newTarget(target, dictTrans);
                targets.put(target.getName(), b);
            }
            return b;
        }

        void prepare() {
            for (MvTarget target : metadata.getTargets().values()) {
                configureTarget(target);
            }
        }

        void configureTarget(MvTarget target) {
            int sourceCount = target.getSources().size();
            if (sourceCount < 1) {
                // constant or expression-based target - nothing to do
                return;
            }
            MvJoinSource source = target.getTopMostSource();
            MvTableInfo.Changefeed cf = source.getChangefeedInfo();
            if (cf == null) {
                LOG.warn("Missing changefeed for main input table `{}`, skipping for target `{}` in handler `{}`.",
                        source.getTableName(), target.getName(), metadata.getName());
                return;
            }
            LOG.info("Configuring handler `{}`, target `{}` ...", metadata.getName(), target.getName());
            // Add sync action for the current target
            ActionSync actionSync = new ActionSync(target, context);
            makeSource(source.getTableInfo()).addAction(actionSync);
            // Put the sync action as a refresh-only for this target
            MvPathGenerator pathGenerator = new MvPathGenerator(target);
            makeTarget(target, makeDictTrans(target, pathGenerator)).addAction(actionSync);
            // Create configuration for other sources
            for (int sourceIndex = 1; sourceIndex < sourceCount; ++sourceIndex) {
                source = target.getSources().get(sourceIndex);
                configureSource(pathGenerator, source);
            }
        }

        void configureSource(MvPathGenerator pg, MvJoinSource source) {
            if (source.getInput() == null || source.getInput().isBatchMode()) {
                return;
            }
            MvTableInfo.Changefeed cf = source.getChangefeedInfo();
            if (cf == null) {
                LOG.info("Missing changefeed for secondary input table `{}`, skipping for target `{}`.",
                        source.getTableName(), pg.getTarget().getName());
                return;
            }
            MvTarget transformation = pg.extractKeysReverse(source);
            if (transformation == null) {
                LOG.info("Keys from input table `{}` cannot be transformed "
                        + "to keys for table `{}`, skipping for target `{}`",
                        source.getTableName(), pg.getTopSourceTableName(),
                        pg.getTarget().getName());
                return;
            }
            MvApplyAction action;
            if (transformation.isKeyOnlyTransformation()) {
                // Can directly transform the input keys to topmost-left key
                action = new ActionKeysTransform(pg.getTarget(), source, transformation, context);
            } else if (transformation.isSingleStepTransformation()
                    && MvTableInfo.ChangefeedMode.BOTH_IMAGES.equals(cf.getMode())) {
                // Can be directly transformed on the changefeed data
                action = new ActionKeysTransform(pg.getTarget(), source, transformation, context);
            } else {
                // The key information has to be grabbed from the database
                action = new ActionKeysGrab(pg.getTarget(), source, transformation, context);
            }
            makeSource(source.getTableInfo()).addAction(action);
        }

        MvTarget makeDictTrans(MvTarget target, MvPathGenerator pathGenerator) {
            var batchSources = target.getSources().stream()
                    .filter(js -> js.isTableKnown())
                    .filter(js -> js.getInput().isBatchMode())
                    .filter(js -> js.isRelated())
                    .toList();
            if (batchSources.isEmpty()) {
                return null;
            }
            var topmostSource = target.getTopMostSource();
            var filter = MvPathGenerator.newFilter();
            filter.add(topmostSource, topmostSource.getKeyColumnNames());
            for (var js : batchSources) {
                filter.add(js, js.getKeyColumnNames());
            }
            return pathGenerator.applyFilter(filter);
        }
    }
}
