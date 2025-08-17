package tech.ydb.mv.model;

import tech.ydb.table.values.Type;

/**
 *
 * @author zinal
 */
public class MvKeyInfo {

    private final String[] fields;
    private final Type[] types;

    public MvKeyInfo(MvTableInfo ti) {
        int count = ti.getKey().size();
        this.fields = new String[count];
        this.types = new Type[count];
        for (int i=0; i<count; ++i) {
            String name = ti.getKey().get(i);
            Type type = ti.getColumns().get(name);
            if (type==null) {
                throw new IllegalStateException("Missing key field information "
                        + "for `" + name + "` in table `" + ti.getName() + "`");
            }
            this.fields[i] = name;
            this.types[i] = type;
        }
    }

}
