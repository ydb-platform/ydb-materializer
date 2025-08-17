package tech.ydb.mv.model;

import tech.ydb.table.description.KeyBound;

/**
 *
 * @author zinal
 */
@SuppressWarnings("rawtypes")
public class MvKeyPrefix extends MvKey {

    public MvKeyPrefix(KeyBound kb, MvKeyInfo info) {
        super(info, makePrefix(kb, info));
    }

    public MvKeyPrefix(String json, MvKeyInfo info) {
        super(info, makePrefix(json, info));
    }

    private static Comparable[] makePrefix(KeyBound kb, MvKeyInfo info) {
        return null;
    }

    private static Comparable[] makePrefix(String json, MvKeyInfo info) {
        return null;
    }

}
