package tech.ydb.mv.model;

import java.util.ArrayList;

/**
 *
 * @author mzinal
 */
public class MvContext {

    private final ArrayList<MvTarget> views = new ArrayList<>();
    private final ArrayList<MvInput> inputs = new ArrayList<>();

    public ArrayList<MvTarget> getViews() {
        return views;
    }

    public ArrayList<MvInput> getInputs() {
        return inputs;
    }

}
