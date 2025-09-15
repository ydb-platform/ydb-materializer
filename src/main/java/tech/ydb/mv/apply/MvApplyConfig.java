package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.HashMap;

import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.parser.MvKeyPathGenerator;

/**
 * The apply configuration includes the settings to process the change records
 * coming from the single input table.
 *
 * @author zinal
 */
class MvApplyConfig {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyConfig.class);

    private final MvTableInfo table;
    private final MvWorkerSelector selector;
    private final MvApplyActionList actions;

    MvApplyConfig(Builder builder) {
        this.table = builder.table;
        this.selector = builder.selector;
        this.actions = new MvApplyActionList(builder.actions);
    }

    public MvTableInfo getTable() {
        return table;
    }

    public MvWorkerSelector getSelector() {
        return selector;
    }

    public MvApplyActionList getActions() {
        return actions;
    }

    public static Builder newBuilder(MvTableInfo table, MvWorkerSelector selector) {
        return new Builder(table, selector);
    }

    static class Builder {
        private final MvTableInfo table;
        private final MvWorkerSelector selector;
        private final ArrayList<MvApplyAction> actions = new ArrayList<>();

        Builder(MvTableInfo table, MvWorkerSelector selector) {
            this.table = table;
            this.selector = selector;
        }

        Builder addAction(MvApplyAction action) {
            actions.add(action);
            return this;
        }

        MvApplyConfig build() {
            return new MvApplyConfig(this);
        }
    }

    static class Configurator {
        final MvActionContext context;
        final MvHandler metadata;
        final HashMap<String, MvApplyConfig.Builder> builders = new HashMap<>();
        final int workersCount;

        public Configurator(MvActionContext context) {
            this.context = context;
            this.metadata = context.getMetadata();
            this.workersCount = context.getSettings().getApplyThreads();
        }

        void build(HashMap<String, MvApplyConfig> applyConfig) {
            prepare();
            fillInto(applyConfig);
        }

        void fillInto(HashMap<String, MvApplyConfig> applyConfig) {
            for (var me : builders.entrySet()) {
                applyConfig.put(me.getKey(), me.getValue().build());
            }
        }

        MvApplyConfig.Builder makeBuilder(MvTableInfo ti) {
            MvApplyConfig.Builder b = builders.get(ti.getName());
            if (b == null) {
                MvWorkerSelector selector = new MvWorkerSelector(ti, workersCount);
                b = MvApplyConfig.newBuilder(ti, selector);
                builders.put(ti.getName(), b);
            }
            return b;
        }

        void prepare() {
            for (MvTarget target : metadata.getTargets().values()) {
                prepareTarget(target);
            }
        }

        void prepareTarget(MvTarget target) {
            int sourceCount = target.getSources().size();
            if (sourceCount < 1) {
                // constant or expression-based target - nothing to do
                return;
            }
            MvJoinSource source = target.getTopMostSource();
            MvTableInfo.Changefeed cf = source.getChangefeedInfo();
            if (cf==null) {
                LOG.warn("Missing changefeed for main input table `{}`, skipping for target `{}` in handler `{}`.",
                        source.getTableName(), target.getName(), metadata.getName());
                return;
            }
            LOG.info("Configuring handler `{}`, target `{}` ...", metadata.getName(), target.getName());
            // Add sync action for the current target
            makeBuilder(source.getTableInfo()).addAction(new ActionSync(target, context));
            if (sourceCount > 1) {
                MvKeyPathGenerator pathGenerator = new MvKeyPathGenerator(target);
                for (int sourceIndex = 1; sourceIndex < sourceCount; ++sourceIndex) {
                    source = target.getSources().get(sourceIndex);
                    prepareComplexTarget(target, pathGenerator, source);
                }
            }
        }

        void prepareComplexTarget(MvTarget target, MvKeyPathGenerator pathGenerator, MvJoinSource source) {
            if (source.getInput()==null || source.getInput().isBatchMode()) {
                return;
            }
            MvTableInfo.Changefeed cf = source.getChangefeedInfo();
            if (cf==null) {
                LOG.info("Missing changefeed for secondary input table `{}`, skipping for target `{}`.",
                        source.getTableName(), target.getName());
                return;
            }
            MvTarget transformation = pathGenerator.extractKeysReverse(source);
            if (transformation==null) {
                LOG.info("Keys from input table `{}` cannot be transformed "
                        + "to keys for table `{}`, skipping for target `{}`",
                        source.getTableName(), pathGenerator.getTopSourceTableName(), target.getName());
                return;
            }
            if (transformation.isKeyOnlyTransformation()) {
                // Can directly transform the input keys to topmost-left key
                makeBuilder(source.getTableInfo())
                        .addAction(new ActionKeysTransform(target, source, transformation, context));
            } else if (transformation.isSingleStepTransformation()
                    && MvTableInfo.ChangefeedMode.BOTH_IMAGES.equals(cf.getMode())) {
                // Can be directly transformed on the changefeed data
                makeBuilder(source.getTableInfo())
                        .addAction(new ActionKeysTransform(target, source, transformation, context));
            } else {
                // The key information has to be grabbed from the database
                makeBuilder(source.getTableInfo())
                        .addAction(new ActionKeysGrab(target, source, transformation, context));
            }
        }
    }
}
