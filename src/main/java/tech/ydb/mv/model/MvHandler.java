package tech.ydb.mv.model;

import java.util.HashMap;

/**
 * Handler is the context for processing multiple changefeed streams
 * to feed the materialized view targets.
 *
 * @author zinal
 */
public class MvHandler implements MvSqlPosHolder {

    private final String name;
    // viewName -> MvTarget
    private final HashMap<String, MvTarget> targets = new HashMap<>();
    // tableName -> MvInput
    private final HashMap<String, MvInput> inputs = new HashMap<>();
    private String consumerName;

    private final MvSqlPos sqlPos;

    public MvHandler(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
    }

    public String getName() {
        return name;
    }

    public HashMap<String, MvTarget> getTargets() {
        return targets;
    }

    public HashMap<String, MvInput> getInputs() {
        return inputs;
    }

    public MvTarget addTarget(MvTarget target) {
        return targets.put(target.getName(), target);
    }

    public MvTarget getTarget(String name) {
        return targets.get(name);
    }

    public MvInput addInput(MvInput input) {
        return inputs.put(input.getTableName(), input);
    }

    public MvInput getInput(String tableName) {
        return inputs.get(tableName);
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

}
