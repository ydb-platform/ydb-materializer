package tech.ydb.mv.impl;

import java.util.HashMap;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author mzinal
 */
public class MvContextValidator {

    private final MvContext context;

    public MvContextValidator(MvContext context) {
        this.context = context;
    }

    public boolean validate() {
        if (! context.isValid()) {
            return false;
        }
        doValidate();
        return context.isValid();
    }

    private void doValidate() {
        checkInputs();
        checkTargets();
        if (context.isValid()) {
            // cross-checks, if other things valid
            checkChangefeeds();
            checkInputsVsTargets();
        }
    }

    private void checkInputs() {
        context.getInputs().forEach(i -> checkInput(i));
    }

    private void checkInput(MvInput i) {
        if (!i.isTableKnown()) {
            context.addIssue(new MvIssue.UnknownInputTable(i));
        }
    }

    private void checkTargets() {
        context.getTargets().forEach(t -> checkTarget(t));
    }

    private void checkTarget(MvTarget t) {
        context.addIssues(t.getSources()
                .stream()
                .filter(js -> !js.isTableKnown())
                .map(js -> new MvIssue.UnknownSourceTable(t, js.getTableName(), js))
                .toList());
    }

    private void checkChangefeeds() {
        context.getInputs().forEach(i -> checkChangefeed(i));
    }

    private void checkChangefeed(MvInput i) {
        if (i.getTableInfo()==null) {
            context.addIssue(new MvIssue.UnknownInputTable(i));
        } else {
            if (i.getChangeFeed()==null
                    || i.getTableInfo().getChangefeeds().get(i.getChangeFeed()) == null) {
                context.addIssue(new MvIssue.UnknownChangefeed(i));
            }
        }
    }

    private void checkInputsVsTargets() {
        final HashMap<String, MvInput> inputs = new HashMap<>();
        context.getInputs().forEach(i -> inputs.put(i.getTableName(), i));
        context.getTargets().forEach(t -> checkTargetVsInputs(t, inputs));
        context.getInputs().forEach(i -> checkInputVsTargets(i));
    }

    private void checkTargetVsInputs(MvTarget t, HashMap<String, MvInput> inputs) {
        for (var s : t.getSources()) {
            if (inputs.get(s.getTableName())==null) {
                context.addIssue(new MvIssue.MissingInput(t, s));
            }
        }
    }

    private void checkInputVsTargets(MvInput i) {
        boolean found = false;
        for (var t : context.getTargets()) {
            for (var s : t.getSources()) {
                if ( i.getTableName().equals(s.getTableName()) ) {
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (!found) {
            context.addIssue(new MvIssue.UselessInput(i));
        }
    }

}
