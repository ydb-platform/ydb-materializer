package tech.ydb.mv.impl;

import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvTarget;

/**
 * MV configuration validation logic.
 *
 * @author zinal
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
        checkHandlers();
        checkTargets();
        if (context.isValid()) {
            // cross-checks, if other things valid
            checkChangefeeds();
            checkInputsVsTargets();
        }
    }

    private void checkHandlers() {
        context.getHandlers().values().forEach(h -> checkHandler(h));
    }

    private void checkHandler(MvHandler h) {
        for (MvInput i : h.getInputs().values()) {
            if (!i.isTableKnown()) {
                context.addIssue(new MvIssue.UnknownInputTable(i));
            }
        }
    }

    private void checkTargets() {
        context.getTargets().values().forEach(t -> checkTarget(t));
    }

    private void checkTarget(MvTarget t) {
        context.addIssues(t.getSources()
                .stream()
                .filter(js -> !js.isTableKnown())
                .map(js -> new MvIssue.UnknownSourceTable(t, js.getTableName(), js))
                .toList());
        context.addIssues(t.getSources()
                .stream()
                .filter(js -> js.isTableKnown())
                .filter(js -> !js.getTableName().equals(js.getTableInfo().getName()))
                .map(js -> new MvIssue.MismatchedSourceTable(t, js))
                .toList());
    }

    private void checkChangefeeds() {
        context.getHandlers().values().stream()
                .flatMap(h -> h.getInputs().values().stream())
                .forEach(i -> checkChangefeed(i));
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
        context.getTargets().values()
                .forEach(t -> checkTargetVsInputs(t));
        context.getHandlers().values().stream()
                .flatMap(h -> h.getInputs().values().stream())
                .forEach(i -> checkInputVsTargets(i));
    }

    private void checkTargetVsInputs(MvTarget t) {
        for (var s : t.getSources()) {
            if (context.getInput(s.getTableName())==null) {
                context.addIssue(new MvIssue.MissingInput(t, s));
            }
        }
    }

    private void checkInputVsTargets(MvInput i) {
        boolean found = false;
        for (var t : context.getTargets().values()) {
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
