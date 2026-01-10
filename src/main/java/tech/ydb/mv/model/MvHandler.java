package tech.ydb.mv.model;

import java.util.HashMap;

/**
 * Handler is the context for processing multiple changefeed streams to feed the
 * materialized view targets.
 *
 * @author zinal
 */
public class MvHandler implements MvSqlPosHolder {

    private final String name;
    // viewName -> MvView
    private final HashMap<String, MvView> views = new HashMap<>();
    // tableName -> MvInput
    private final HashMap<String, MvInput> inputs = new HashMap<>();
    private String consumerName;

    private final MvSqlPos sqlPos;

    public MvHandler(String name, MvSqlPos sqlPos) {
        this.name = name;
        this.sqlPos = sqlPos;
    }

    public MvHandler(String name) {
        this(name, MvSqlPos.EMPTY);
    }

    public String getName() {
        return name;
    }

    public HashMap<String, MvView> getViews() {
        return views;
    }

    public HashMap<String, MvInput> getInputs() {
        return inputs;
    }

    public MvView addView(MvView v) {
        return views.put(v.getName(), v);
    }

    public MvView getView(String name) {
        return views.get(name);
    }

    public MvViewExpr getPart(String name, String alias) {
        MvView v = getView(name);
        if (v == null) {
            return null;
        }
        return v.getParts().get(alias);
    }

    public boolean containsPart(MvViewExpr part) {
        if (part == null) {
            return false;
        }
        return part == getPart(part.getName(), part.getAlias());
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

    public String getConsumerNameAlways() {
        if (consumerName != null && consumerName.length() > 0) {
            return consumerName;
        }
        if (!name.contains("/")) {
            return name;
        }
        String[] parts = name.split("[/]");
        for (int pos = parts.length; pos > 0; --pos) {
            if (parts[pos - 1].length() > 0) {
                return parts[pos - 1];
            }
        }
        return "ydb$mv";
    }

    @Override
    public MvSqlPos getSqlPos() {
        return sqlPos;
    }

}
