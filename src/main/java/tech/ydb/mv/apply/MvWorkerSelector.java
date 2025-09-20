package tech.ydb.mv.apply;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.data.MvKeyPrefix;
import tech.ydb.mv.model.MvTableInfo;

/**
 * The utility algorithm which can choose the proper worker based on the record
 * key.
 *
 * @author zinal
 */
class MvWorkerSelector {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvWorkerSelector.class);

    private final MvTableInfo tableInfo;
    private final int workerCount;
    private final AtomicReference<Chooser> chooser;

    public MvWorkerSelector(MvTableInfo tableInfo, int workerCount) {
        this.tableInfo = tableInfo;
        this.workerCount = (workerCount > 0) ? workerCount : 1;
        this.chooser = new AtomicReference<>(new Chooser(this.workerCount));
    }

    public MvTableInfo getTableInfo() {
        return tableInfo;
    }

    public MvKeyInfo getKeyInfo() {
        return tableInfo.getKeyInfo();
    }

    public int getWorkerCount() {
        return workerCount;
    }

    /**
     * For test purposes only.
     *
     * @return The current chooser instance.
     */
    public Chooser getChooser() {
        return chooser.get();
    }

    public void refresh(TableClient tableClient) {
        if (workerCount < 2) {
            // No need to describe anything: we have a single worker.
            return;
        }
        Chooser newChooser;
        try {
            newChooser = load(tableClient);
        } catch (Exception ex) {
            LOG.error("Failed to refresh the partitioning setup for table {}",
                    tableInfo.getPath(), ex);
            return;
        }
        chooser.set(newChooser);
    }

    public int choose(MvKey key) {
        return chooser.get().choose(key);
    }

    private Chooser load(TableClient tableClient) {
        // Grab the prefixes for the table partitions.
        MvKeyPrefix[] prefixes = readPrefixes(tableClient);
        Chooser c = new Chooser(workerCount);
        int pcount = prefixes.length;
        if (pcount + 1 < workerCount) {
            // we can assign a distinct worker to each prefix
            for (int i = 0; i < pcount; ++i) {
                c.items.put(prefixes[i], i + 1);
            }
        } else {
            for (int i = 0; i < pcount; ++i) {
                // 0..workerCount, non-decreasing
                int worker = (i * workerCount) / pcount;
                c.items.put(prefixes[i], worker);
            }
        }
        return c;
    }

    protected MvKeyPrefix[] readPrefixes(TableClient tableClient) {
        TableDescription desc;
        DescribeTableSettings dts = new DescribeTableSettings();
        dts.setIncludeShardKeyBounds(true);
        try (Session session = tableClient
                .createSession(Duration.ofSeconds(10)).join().getValue()) {
            desc = session.describeTable(tableInfo.getPath(), dts).join().getValue();
        }
        return desc.getKeyRanges().stream()
                .filter(kr -> kr.getTo().isPresent())
                .map(kr -> kr.getTo().get())
                .map(kb -> new MvKeyPrefix(kb, tableInfo.getKeyInfo()))
                .toArray(MvKeyPrefix[]::new);
    }

    public static final class Chooser {

        private final int workerCount;
        private final TreeMap<MvKeyPrefix, Integer> items = new TreeMap<>();

        public Chooser(int workerCount) {
            this.workerCount = workerCount;
        }

        public TreeMap<MvKeyPrefix, Integer> getItems() {
            return items;
        }

        public int choose(MvKey key) {
            Map.Entry<MvKeyPrefix, Integer> item = items.higherEntry(key);
            if (item == null) {
                return workerCount - 1;
            }
            return item.getValue();
        }

    }

}
