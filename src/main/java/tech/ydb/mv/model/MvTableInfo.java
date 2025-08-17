package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.Type;

/**
 *
 * @author zinal
 */
public class MvTableInfo {

    private final String name;
    private final Map<String, Type> columns;
    private final List<String> key;
    private final MvKeyInfo keyInfo;
    private final Map<String, Index> indexes;
    private final Map<String, Changefeed> changefeeds;

    public MvTableInfo(String name, TableDescription td) {
        LinkedHashMap<String, Type> theColumns = new LinkedHashMap<>();
        for (var c : td.getColumns()) {
            theColumns.putLast(c.getName(), c.getType());
        }
        ArrayList<String> theKey = new ArrayList<>();
        for (String k : td.getPrimaryKeys()) {
            theKey.add(k);
        }
        HashMap<String, Index> theIndexes = new HashMap<>();
        for (var i : td.getIndexes()) {
            var idx = new MvTableInfo.Index(i.getName(), i.getColumns());
            theIndexes.put(idx.getName(), idx);
        }
        HashMap<String, Changefeed> theFeeds = new HashMap<>();
        for (var c : td.getChangefeeds()) {
            if (! tech.ydb.table.settings.Changefeed.Format.JSON.equals(c.getFormat())) {
                // skipping changefeeds having unsupported format
                continue;
            }
            var cf = new MvTableInfo.Changefeed(c.getName());
            theFeeds.put(cf.getName(), cf);
        }
        this.name = name;
        this.columns = Collections.unmodifiableMap(theColumns);
        this.key = Collections.unmodifiableList(theKey);
        this.indexes = Collections.unmodifiableMap(theIndexes);
        this.changefeeds = Collections.unmodifiableMap(theFeeds);
        this.keyInfo = new MvKeyInfo(this);
    }

    private MvTableInfo(String name, Map<String, Type> columns,
            List<String> key, Map<String, Index> indexes,
            Map<String, Changefeed> changefeeds) {
        this.name = name;
        this.columns = Collections.unmodifiableMap(columns);
        this.key = Collections.unmodifiableList(key);
        this.indexes = Collections.unmodifiableMap(indexes);
        this.changefeeds = Collections.unmodifiableMap(changefeeds);
        this.keyInfo = new MvKeyInfo(this);
    }

    public String getName() {
        return name;
    }

    public Map<String, Type> getColumns() {
        return columns;
    }

    public List<String> getKey() {
        return key;
    }

    public MvKeyInfo getKeyInfo() {
        return keyInfo;
    }

    public Map<String, Index> getIndexes() {
        return indexes;
    }

    public Map<String, Changefeed> getChangefeeds() {
        return changefeeds;
    }

    public static Builder newBuilder(String name) {
        return new Builder(name);
    }

    public static final class Index {
        private final String name;
        private final List<String> columns;

        public Index(String name, List<String> columns) {
            this.name = name;
            this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        }

        public Index(String name, String[] columns) {
            this.name = name;
            this.columns = Collections.unmodifiableList(Arrays.asList(columns));
        }

        public String getName() {
            return name;
        }

        public List<String> getColumns() {
            return columns;
        }
    }

    public static final class Changefeed {
        private final String name;

        public Changefeed(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final class Builder {
        private final String name;
        private final LinkedHashMap<String, Type> columns = new LinkedHashMap<>();
        private final List<String> key = new ArrayList<>();
        private final Map<String, Index> indexes = new HashMap<>();
        private final Map<String, Changefeed> changefeeds = new HashMap<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder addColumn(String name, Type type) {
            columns.putLast(name, type);
            return this;
        }

        public Builder addKey(String name) {
            key.add(name);
            return this;
        }

        public Builder addIndex(String name, String... column) {
            Index idx = new Index(name, column);
            indexes.put(idx.getName(), idx);
            return this;
        }

        public Builder addChangefeed(String name) {
            Changefeed cf = new Changefeed(name);
            changefeeds.put(cf.getName(), cf);
            return this;
        }

        public MvTableInfo build() {
            return new MvTableInfo(name, columns, key, indexes, changefeeds);
        }
    }

}
