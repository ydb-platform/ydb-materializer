package tech.ydb.mv.data;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

/**
 * Changes for all dictionaries which are used in the particular handler.
 *
 * @author zinal
 */
public class MvChangesMultiDict {

    private final HashMap<String, MvChangesSingleDict> items = new HashMap<>();

    public Collection<MvChangesSingleDict> getItems() {
        return Collections.unmodifiableCollection(items.values());
    }

    public MvChangesMultiDict addItem(MvChangesSingleDict sd) {
        items.put(sd.getTableName(), sd);
        return this;
    }

}
