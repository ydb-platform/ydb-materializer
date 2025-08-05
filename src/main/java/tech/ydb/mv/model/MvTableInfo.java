package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import tech.ydb.table.values.Type;

/**
 *
 * @author mzinal
 */
public class MvTableInfo {

    private final String name;
    private final LinkedHashMap<String, Type> columns = new LinkedHashMap<>();
    private final ArrayList<String> key = new ArrayList<>();
    private final HashMap<String, Index> indexes = new HashMap<>();
    private final HashMap<String, Changefeed> changefeeds = new HashMap<>();

    public MvTableInfo(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public LinkedHashMap<String, Type> getColumns() {
        return columns;
    }

    public ArrayList<String> getKey() {
        return key;
    }

    public Map<String, Index> getIndexes() {
        return indexes;
    }

    public Map<String, Changefeed> getChangefeeds() {
        return changefeeds;
    }

    public static final class Index {
        private final String name;
        private final ArrayList<String> columns = new ArrayList<>();

        public Index(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public ArrayList<String> getColumns() {
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

}
