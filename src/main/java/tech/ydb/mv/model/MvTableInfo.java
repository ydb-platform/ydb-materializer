package tech.ydb.mv.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import tech.ydb.table.values.Type;

/**
 *
 * @author mzinal
 */
public class MvTableInfo {

    private final String name;
    private final LinkedHashMap<String, Type> columns = new LinkedHashMap<>();
    private final ArrayList<String> key = new ArrayList<>();
    private final HashMap<String, ArrayList<String>> indexes = new HashMap<>();

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

    public HashMap<String, ArrayList<String>> getIndexes() {
        return indexes;
    }

}
