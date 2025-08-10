package tech.ydb.mv.model;

import java.util.HashMap;

/**
 * Handler is the context for processing multiple changefeed streams.
 * @author zinal
 */
public class MvHandler implements MvSqlPosHolder {

    private final String name;
    // tableName -> MvInput
    private final HashMap<String, MvInput> inputs = new HashMap<>();
    private final MvSqlPos sqlPos;

    public MvHandler(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
    }

    public String getName() {
        return name;
    }

    public HashMap<String, MvInput> getInputs() {
        return inputs;
    }

    public MvInput addInput(MvInput input) {
        return inputs.put(input.getTableName(), input);
    }

    public MvInput getInput(String tableName) {
        return inputs.get(tableName);
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

}
