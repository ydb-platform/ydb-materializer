package tech.ydb.mv.apply;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

import tech.ydb.mv.MvConfig;
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
    private final MvConfig.PartitioningStrategy partitioning;

    public MvWorkerSelector(MvTableInfo tableInfo, int workerCount, MvConfig.PartitioningStrategy partitioning) {
        this.tableInfo = tableInfo;
        this.workerCount = (workerCount > 0) ? workerCount : 1;
        this.chooser = new AtomicReference<>(
                MvConfig.PartitioningStrategy.HASH.equals(partitioning) ?
                new ChooserHash(this.workerCount) :
                new ChooserRange(this.workerCount)
        );
        this.partitioning = partitioning;
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
        if (MvConfig.PartitioningStrategy.HASH.equals(partitioning)) {
            // Hash partitioning does not require any describes.
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
        ChooserRange c = new ChooserRange(workerCount);
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

    public interface Chooser {
        int choose(MvKey key);
    }

    public static final class ChooserRange implements Chooser {

        private final int workerCount;
        private final TreeMap<MvKeyPrefix, Integer> items = new TreeMap<>();

        public ChooserRange(int workerCount) {
            this.workerCount = workerCount;
        }

        public TreeMap<MvKeyPrefix, Integer> getItems() {
            return items;
        }

        @Override
        public int choose(MvKey key) {
            Map.Entry<MvKeyPrefix, Integer> item = items.higherEntry(key);
            if (item == null) {
                return workerCount - 1;
            }
            return item.getValue();
        }

    }

    public static final class ChooserHash implements Chooser {

        private final int workerCount;

        public ChooserHash(int workerCount) {
            this.workerCount = workerCount;
        }

        @Override
        public int choose(MvKey key) {
            return (int) (Integer.toUnsignedLong(key.hashCode()) % workerCount);
        }

    }

}
