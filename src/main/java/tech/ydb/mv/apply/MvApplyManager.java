package tech.ydb.mv.apply;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import tech.ydb.table.TableClient;

import tech.ydb.mv.MvJobContext;
import tech.ydb.mv.model.MvChangeRecord;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.util.YdbMisc;

/**
 * The apply manager processes the changes in the context of a single handler.
 * Multiple apply managers can run in a single application.
 *
 * @author zinal
 */
public class MvApplyManager {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvApplyManager.class);

    private final MvActionContext context;
    private final MvApplyWorker[] workers;

    // table name -> table apply configuration data
    private final HashMap<String, MvApplyConfig> applyConfig = new HashMap<>();

    public MvApplyManager(MvJobContext context) {
        this.context = new MvActionContext(context, this);
        int workerCount = context.getSettings().getApplyThreads();
        this.workers = new MvApplyWorker[workerCount];
        for (int i=0; i<workerCount; ++i) {
            workers[i] = new MvApplyWorker(this, i);
        }
        buildConfig();
    }

    private MvApplyConfig makeTableConfig(MvTableInfo ti) {
        MvApplyConfig mac = applyConfig.get(ti.getName());
        if (mac==null) {
            mac = new MvApplyConfig(ti, workers.length);
            applyConfig.put(ti.getName(), mac);
        }
        return mac;
    }

    private void buildConfig() {
        MvHandler handler = context.getMetadata();
        for (MvTarget target : handler.getTargets().values()) {
            int sourceCount = target.getSources().size();
            if (sourceCount < 1) {
                // constant or expression-based target - nothing to do
                continue;
            }
            // Add sync action for the current target
            MvJoinSource src = target.getTopMostSource();
            MvTableInfo.Changefeed cf = src.getChangefeedInfo();
            if (cf==null) {
                LOG.warn("Missing changefeed for main input table {}, skipping for target {} in handler {}.",
                        src.getTableName(), target.getName(), handler.getName());
                continue;
            }
            LOG.info("Configuring handler {}, target {} ...", handler.getName(), target.getName());
            makeTableConfig(src.getTableInfo())
                    .addAction(new MvSynchronize(target, context));
            if (sourceCount < 2) {
                // single-source target, no joins
                continue;
            }
            MvKeyPathGenerator pathGenerator = new MvKeyPathGenerator(target);
            for (int sourceIndex = 1; sourceIndex < sourceCount; ++sourceIndex) {
                src = target.getSources().get(sourceIndex);
                cf = src.getChangefeedInfo();
                if (cf==null) {
                    LOG.info("Missing changefeed for secondary input table {}, skipping for target {}.",
                            src.getTableName(), target.getName());
                    continue;
                }
                MvTarget transformation = pathGenerator.generate(src);
                if (transformation==null) {
                    LOG.info("Keys from input table {} cannot be transformed "
                            + "to keys for table {}, skipping for target {}",
                            src.getTableName(), pathGenerator.getTopMostSource().getTableName(), target.getName());
                    continue;
                }
                if (transformation.isKeyOnlyTransformation()) {
                    // Can directly transform the input keys to topmost-left key
                    makeTableConfig(src.getTableInfo())
                            .addAction(new MvKeysTransform(target, src, transformation, context));
                } else if (transformation.isSingleStepTransformation()
                        && MvTableInfo.ChangefeedMode.BOTH_IMAGES.equals(cf.getMode())) {
                    // Can be directly transformed on the changefeed data
                    makeTableConfig(src.getTableInfo())
                            .addAction(new MvKeysTransform(target, src, transformation, context));
                } else {
                    // The key information has to be grabbed from the database
                    makeTableConfig(src.getTableInfo())
                            .addAction(new MvKeysGrab(target, src, transformation, context));
                }
            }
        }
    }

    public boolean isRunning() {
        return context.isRunning();
    }

    public int getWorkersCount() {
        return workers.length;
    }

    public MvHandlerSettings getSettings() {
        return context.getSettings();
    }

    public MvApplyWorker getWorker(int index) {
        if (index < 0) {
            index = -1 * index;
        }
        index = index % workers.length;
        return workers[index];
    }

    /**
     * @return The number of workers locked in retry logic (so not progressing)
     */
    public int getLockedWorkersCount() {
        int count = 0;
        for (MvApplyWorker w : workers) {
            if (w.isLocked()) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * @return true if any of the workers is locked by the error being retried, false otherwise
     */
    public boolean isLocked() {
        return (getLockedWorkersCount() > 0);
    }

    /**
     * Refresh the worker selector setup by reading the fresh partitioning data.
     *
     * @param tableClient Table client is needed to describe the source tables.
     */
    public void refreshSelectors(TableClient tableClient) {
        applyConfig.values().forEach(ac -> ac.getSelector().refresh(tableClient));
    }

    /**
     * Used by the controller to start the apply worker threads.
     * No need for explicit stop method here - the threads stop when
     * the controller reports itself as stopped via isRunning() method.
     */
    public void start() {
        for (MvApplyWorker w : workers) {
            w.start();
        }
        LOG.info("Started {} apply workers.", workers.length);
    }

    /**
     * Insert the input data to the queues of the proper workers.
     *
     * In case some of the queues are full, wait until the capacity
     * becomes available, or until the update manager is stopped.
     *
     * @param changes The change records to be submitted for processing.
     * @param commitHandler
     * @return true, if all keys went to the queue, and false otherwise.
     */
    public boolean submit(Collection<MvChangeRecord> changes, MvCommitHandler commitHandler) {
        if (changes.isEmpty()) {
            return true;
        }
        int count = changes.size();
        String tableName = changes.iterator().next().getKey().getTableInfo().getName();
        MvApplyConfig apply = applyConfig.get(tableName);
        if (apply==null) {
            commitHandler.apply(count);
            LOG.warn("Skipping {} change records for unexpected table {}", count, tableName);
            return true;
        }
        ArrayList<MvApplyTask> curr = new ArrayList<>(count);
        ArrayList<MvApplyTask> next = new ArrayList<>(count);
        for (MvChangeRecord change : changes) {
            if (! tableName.equals(change.getKey().getTableInfo().getName())) {
                throw new IllegalArgumentException("Mixed input tables on submission");
            }
            curr.add(new MvApplyTask(change, apply, commitHandler));
        }
        while (isRunning()) {
            for (MvApplyTask task : curr) {
                if (! getWorker(task.getWorkerId()).submit(task)) {
                    // add for re-processing
                    next.add(task);
                }
            }
            if (next.isEmpty()) {
                // Everything submitted
                return true;
            }
            // Switch the working set.
            curr.clear();
            ArrayList<MvApplyTask> temp = curr;
            curr = next;
            next = temp;
            // Allow the queues to get released.
            YdbMisc.randomSleep(10L, 50L);
        }
        // Exit without processing all the inputs
        return false;
    }

}
