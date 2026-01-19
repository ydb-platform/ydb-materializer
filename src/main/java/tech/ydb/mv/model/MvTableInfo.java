package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.Type;

/**
 *
 * @author zinal
 */
public class MvTableInfo {

    public static final String PK_INDEX = "PRIMARY KEY";

    private final String name;
    private final String path;
    private final Map<String, Type> columns;
    private final List<String> key;
    private final MvKeyInfo keyInfo;
    private final Map<String, Index> indexes;
    private final Map<String, Changefeed> changefeeds;

    public MvTableInfo(String name, String path, TableDescription td) {
        LinkedHashMap<String, Type> theColumns = new LinkedHashMap<>();
        for (var c : td.getColumns()) {
            theColumns.remove(c.getName());
            theColumns.put(c.getName(), c.getType());
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
            final ChangefeedMode mode;
            switch (c.getMode()) {
                case NEW_AND_OLD_IMAGES:
                    mode = ChangefeedMode.BOTH_IMAGES;
                    break;
                case NEW_IMAGE:
                    mode = ChangefeedMode.NEW_IMAGE;
                    break;
                case OLD_IMAGE:
                    mode = ChangefeedMode.OLD_IMAGE;
                    break;
                case UPDATES:
                    mode = ChangefeedMode.UPDATES;
                    break;
                default:
                    mode = ChangefeedMode.KEYS_ONLY;
            }
            var cf = new MvTableInfo.Changefeed(c.getName(), mode);
            theFeeds.put(cf.getName(), cf);
        }
        this.name = name;
        this.path = path;
        this.columns = Collections.unmodifiableMap(theColumns);
        this.key = Collections.unmodifiableList(theKey);
        this.indexes = Collections.unmodifiableMap(theIndexes);
        this.changefeeds = Collections.unmodifiableMap(theFeeds);
        this.keyInfo = new MvKeyInfo(this);
    }

    private MvTableInfo(String name, String path, Map<String, Type> columns,
            List<String> key, Map<String, Index> indexes,
            Map<String, Changefeed> changefeeds) {
        this.name = name;
        this.path = path;
        this.columns = Collections.unmodifiableMap(columns);
        this.key = Collections.unmodifiableList(key);
        this.indexes = Collections.unmodifiableMap(indexes);
        this.changefeeds = Collections.unmodifiableMap(changefeeds);
        this.keyInfo = new MvKeyInfo(this);
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.name);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvTableInfo other = (MvTableInfo) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.path, other.path)) {
            return false;
        }
        if (!Objects.equals(this.columns, other.columns)) {
            return false;
        }
        if (!Objects.equals(this.key, other.key)) {
            return false;
        }
        if (!Objects.equals(this.keyInfo, other.keyInfo)) {
            return false;
        }
        if (!Objects.equals(this.indexes, other.indexes)) {
            return false;
        }
        return Objects.equals(this.changefeeds, other.changefeeds);
    }

    /**
     * Find the index which covers the specified columns as a key prefix.
     * @param columns The columns to be handled
     * @return index name, if one exists, or null otherwise
     */
    public String findProperIndex(List<String> columns) {
        if (indexCoversColumns(key, columns)) {
            return PK_INDEX;
        }
        for (Map.Entry<String, Index> me : indexes.entrySet()) {
            if (indexCoversColumns(me.getValue().getColumns(), columns)) {
                return me.getKey();
            }
        }
        return null;
    }

    /**
     * Check if an index covers all required columns. An index covers the
     * columns if all required columns appear as a prefix of the index columns.
     */
    private static boolean indexCoversColumns(List<String> indexColumns, List<String> requiredColumns) {
        HashSet<String> required = new HashSet<>(requiredColumns);
        if (indexColumns.size() < required.size()) {
            return false;
        }
        for (String indexColumn : indexColumns) {
            if (! required.remove(indexColumn)) {
                break;
            }
        }
        return required.isEmpty();
    }

    public static Builder newBuilder(String name) {
        return new Builder(name, name);
    }

    public static Builder newBuilder(String name, String path) {
        return new Builder(name, path);
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

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 67 * hash + Objects.hashCode(this.name);
            hash = 67 * hash + Objects.hashCode(this.columns);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Index other = (Index) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            return Objects.equals(this.columns, other.columns);
        }
    }

    public static final class Changefeed {
        private final String name;
        private final ChangefeedMode mode;
        private final HashSet<String> consumers = new HashSet<>();

        public Changefeed(String name, ChangefeedMode mode) {
            this.name = name;
            this.mode = mode;
        }

        public String getName() {
            return name;
        }

        public ChangefeedMode getMode() {
            return mode;
        }

        public Set<String> getConsumers() {
            return consumers;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 79 * hash + Objects.hashCode(this.name);
            hash = 79 * hash + Objects.hashCode(this.mode);
            hash = 79 * hash + Objects.hashCode(this.consumers);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Changefeed other = (Changefeed) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (this.mode != other.mode) {
                return false;
            }
            return Objects.equals(this.consumers, other.consumers);
        }
    }

    public static enum ChangefeedMode {
        KEYS_ONLY,
        UPDATES,
        NEW_IMAGE,
        OLD_IMAGE,
        BOTH_IMAGES
    }

    public static final class Builder {
        private final String name;
        private final String path;
        private final LinkedHashMap<String, Type> columns = new LinkedHashMap<>();
        private final List<String> key = new ArrayList<>();
        private final Map<String, Index> indexes = new HashMap<>();
        private final Map<String, Changefeed> changefeeds = new HashMap<>();

        private Builder(String name, String path) {
            this.name = name;
            this.path = path;
        }

        public Builder addColumn(String name, Type type) {
            columns.remove(name);
            columns.put(name, type);
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

        public Builder addChangefeed(String name, ChangefeedMode mode, String consumer) {
            Changefeed cf = new Changefeed(name, mode);
            if (consumer!=null) {
                cf.getConsumers().add(consumer);
            }
            changefeeds.put(cf.getName(), cf);
            return this;
        }

        public Builder addChangefeed(String name, ChangefeedMode mode) {
            return addChangefeed(name, mode, null);
        }

        public Builder addChangefeed(String name) {
            return addChangefeed(name, ChangefeedMode.KEYS_ONLY);
        }

        public MvTableInfo build() {
            return new MvTableInfo(name, path, columns, key, indexes, changefeeds);
        }
    }

}
