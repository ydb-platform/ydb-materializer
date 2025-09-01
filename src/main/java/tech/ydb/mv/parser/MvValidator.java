package tech.ydb.mv.parser;

import java.util.List;

import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.model.MvTableInfo;

/**
 * MV configuration validation logic.
 *
 * @author zinal
 */
public class MvValidator {

    private final MvContext context;

    public MvValidator(MvContext context) {
        this.context = context;
    }

    public boolean validate() {
        if (!context.isValid()) {
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
        if (h.getTargets().isEmpty()) {
            context.addIssue(new MvIssue.EmptyHandler(h, MvIssue.EmptyHandlerType.NO_TARGETS));
        }
        if (h.getInputs().isEmpty()) {
            context.addIssue(new MvIssue.EmptyHandler(h, MvIssue.EmptyHandlerType.NO_INPUTS));
        }
        for (MvInput i : h.getInputs().values()) {
            if (!i.isTableKnown()) {
                context.addIssue(new MvIssue.UnknownInputTable(i));
            }
        }
    }

    private void checkTargets() {
        for (MvTarget mt : context.getTargets().values()) {
            checkTarget(mt);
            checkJoinIndexes(mt);
        }
    }

    private void checkTarget(MvTarget mt) {
        context.addIssues(mt.getSources()
                .stream()
                .filter(js -> !js.isTableKnown())
                .map(js -> new MvIssue.UnknownSourceTable(mt, js.getTableName(), js))
                .toList());
        context.addIssues(mt.getSources()
                .stream()
                .filter(js -> js.isTableKnown())
                .filter(js -> !js.getTableName().equals(js.getTableInfo().getName()))
                .map(js -> new MvIssue.MismatchedSourceTable(mt, js))
                .toList());
        // Validate that the target is used in no more than one handler.
        MvHandler firstHandler = null;
        for (MvHandler mh : context.getHandlers().values()) {
            if (mh.getTarget(mt.getName()) != null) {
                if (firstHandler == null) {
                    firstHandler = mh;
                } else {
                    context.addIssue(new MvIssue.TargetMultipleHandlers(mt, firstHandler, mh));
                }
            }
        }
        if (firstHandler == null) {
            // Unused/unreferenced target, so issue a warning
            context.addIssue(new MvIssue.UselessTarget(mt));
        }
        mt.getSources().forEach(src -> checkJoinConditions(mt, src));
    }

    private void checkJoinConditions(MvTarget mt, MvJoinSource src) {
        for (MvJoinCondition cond : src.getConditions()) {
            if ((cond.getFirstAlias() == null && cond.getFirstLiteral() == null)
                    || (cond.getSecondAlias() == null && cond.getSecondLiteral() == null)
                    || (cond.getFirstAlias() == null && cond.getSecondAlias() == null)) {
                context.addIssue(new MvIssue.IllegalJoinCondition(mt, src, cond));
            } else if (!src.getTableAlias().equals(cond.getFirstAlias())
                    && !src.getTableAlias().equals(cond.getSecondAlias())) {
                // TODO: maybe a different issue with better explanation
                context.addIssue(new MvIssue.IllegalJoinCondition(mt, src, cond));
            }
        }
    }

    private void checkJoinIndexes(MvTarget mt) {
        // Check each join source for missing indexes on join columns
        for (MvJoinSource src : mt.getSources()) {
            // Skip MAIN source - we only check right parts of joins (INNER, LEFT)
            if (src.getMode() == null || src.getMode() == MvJoinMode.MAIN) {
                continue;
            }

            // Skip if table info is not available
            if (!src.isTableKnown() || src.getTableInfo() == null) {
                continue;
            }

            // Collect all columns used in join conditions for this source
            List<String> joinColumns = new java.util.ArrayList<>();
            for (MvJoinCondition cond : src.getConditions()) {
                // Check if this condition references the current source and collect the column
                if (src.getTableAlias().equals(cond.getFirstAlias()) && cond.getFirstColumn() != null) {
                    if (!joinColumns.contains(cond.getFirstColumn())) {
                        joinColumns.add(cond.getFirstColumn());
                    }
                } else if (src.getTableAlias().equals(cond.getSecondAlias()) && cond.getSecondColumn() != null) {
                    if (!joinColumns.contains(cond.getSecondColumn())) {
                        joinColumns.add(cond.getSecondColumn());
                    }
                }
            }

            // If no join columns found, skip
            if (joinColumns.isEmpty()) {
                continue;
            }

            // Check if there's an index covering all join columns
            boolean indexFound = false;
            MvTableInfo tableInfo = src.getTableInfo();

            // Check primary key first
            if (indexCoversColumns(tableInfo.getKey(), joinColumns)) {
                indexFound = true;
            }

            // Check secondary indexes
            if (!indexFound) {
                for (MvTableInfo.Index index : tableInfo.getIndexes().values()) {
                    if (indexCoversColumns(index.getColumns(), joinColumns)) {
                        indexFound = true;
                        break;
                    }
                }
            }

            // If no covering index found, add warning
            if (!indexFound) {
                context.addIssue(new MvIssue.MissingJoinIndex(mt, src, joinColumns));
            }
        }
    }

    /**
     * Check if an index covers all required columns. An index covers the
     * columns if all required columns appear as a prefix of the index columns.
     */
    private boolean indexCoversColumns(List<String> indexColumns, List<String> requiredColumns) {
        if (indexColumns.size() < requiredColumns.size()) {
            return false;
        }

        // Check if all required columns appear as a prefix in the index
        for (int i = 0; i < requiredColumns.size(); i++) {
            if (i >= indexColumns.size() || !requiredColumns.get(i).equals(indexColumns.get(i))) {
                return false;
            }
        }

        return true;
    }

    private void checkChangefeeds() {
        context.getHandlers().values().stream()
                .flatMap(h -> h.getInputs().values().stream())
                .forEach(i -> checkChangefeed(i));
    }

    private void checkChangefeed(MvInput i) {
        if (i.getTableInfo() == null) {
            context.addIssue(new MvIssue.UnknownInputTable(i));
        } else {
            if (i.getChangefeed() == null
                    || i.getTableInfo().getChangefeeds().get(i.getChangefeed()) == null) {
                context.addIssue(new MvIssue.UnknownChangefeed(i));
            }
        }
    }

    private void checkInputsVsTargets() {
        for (MvHandler mh : context.getHandlers().values()) {
            for (MvTarget mt : mh.getTargets().values()) {
                checkTargetVsInputs(mh, mt);
            }
            for (MvInput mi : mh.getInputs().values()) {
                checkInputVsTargets(mh, mi);
            }
        }
    }

    private void checkTargetVsInputs(MvHandler mh, MvTarget mt) {
        for (var joinSource : mt.getSources()) {
            if (mh.getInput(joinSource.getTableName()) == null) {
                context.addIssue(new MvIssue.MissingInput(mt, joinSource));
            }
        }
    }

    private void checkInputVsTargets(MvHandler mh, MvInput i) {
        boolean found = false;
        for (var mt : mh.getTargets().values()) {
            for (var s : mt.getSources()) {
                if (i.getTableName().equals(s.getTableName())) {
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
