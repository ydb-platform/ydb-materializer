package tech.ydb.mv.impl;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.TableClient;

import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvKeyPrefix;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.table.Session;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

/**
 * The utility algorithm which can choose the proper worker based on the record key.
 *
 * @author zinal
 */
public class MvSelectWorker {

    private final TableClient tableClient;
    private final MvTableInfo tableInfo;
    private final int workerCount;
    private final AtomicReference<Chooser> chooser;

    public MvSelectWorker(TableClient tableClient, MvTableInfo tableInfo, int workerCount) {
        this.tableClient = tableClient;
        this.tableInfo = tableInfo;
        this.workerCount = workerCount;
        this.chooser = new AtomicReference<>(new Chooser());
    }

    public void refresh() {
        Chooser newChooser = load();
        chooser.set(newChooser);
    }

    public int choose(MvKey key) {
        return chooser.get().choose(key);
    }

    private Chooser load() {
        if (workerCount < 2) {
            // No need to describe anything: we have a single worker.
            return new Chooser();
        }
        // Grab the prefixes for the table partitions.
        MvKeyPrefix[] prefixes = readPrefixes();
        Chooser c = new Chooser();
        int pcount = prefixes.length;
        if (pcount + 1 < workerCount) {
            // we can assign a distinct worker to each prefix
            for (int i = 0; i < pcount; ++i) {
                c.items.put(prefixes[i], i+1);
            }
        } else {
            for (int i = 0; i < pcount; ++i) {
                // 1..workerCount, non-decreasing
                int worker = (i * (workerCount-1)) / pcount + 1;
                c.items.put(prefixes[i], worker);
            }
        }
        return c;
    }

    private MvKeyPrefix[] readPrefixes() {
        TableDescription desc;
        DescribeTableSettings dts = new DescribeTableSettings();
        dts.setIncludeShardKeyBounds(true);
        try (Session session = tableClient
                .createSession(Duration.ofSeconds(10)).join().getValue()) {
            desc = session.describeTable(tableInfo.getPath(), dts).join().getValue();
        }
        return desc.getKeyRanges().stream()
                .filter(kr -> kr.getFrom().isPresent())
                .map(kr -> kr.getFrom().get())
                .map(kb -> new MvKeyPrefix(kb, tableInfo.getKeyInfo()))
                .toArray(MvKeyPrefix[]::new);
    }

    public static final class Chooser {

        private final TreeMap<MvKeyPrefix, Integer> items = new TreeMap<>();

        public TreeMap<MvKeyPrefix, Integer> getItems() {
            return items;
        }

        public int choose(MvKey key) {
            Map.Entry<MvKeyPrefix, Integer> item = items.ceilingEntry(key);
            if (item==null) {
                return 0;
            }
            return item.getValue();
        }

    }

}
