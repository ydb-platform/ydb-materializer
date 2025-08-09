package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 * Handler is the context for processing multiple changefeed streams.
 * @author zinal
 */
public class MvHandler {

    private final String name;
    private final ArrayList<MvInput> inputs = new ArrayList<>();

    public MvHandler(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public ArrayList<MvInput> getInputs() {
        return inputs;
    }

}
