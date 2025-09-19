package tech.ydb.mv.parser;

import java.util.List;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * MV configuration validation logic.
 *
 * @author zinal
 */
public class MvValidateBasic {

    private final MvMetadata context;

    public MvValidateBasic(MvMetadata context) {
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
            checkKeyExtractionIndexes(mt);
        }
    }

    private void checkTarget(MvTarget mt) {
        if (mt.getTableInfo()==null) {
            context.addIssue(new MvIssue.MissingTargetTable(mt));
        }
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
        mt.getColumns().forEach(column -> checkTargetOutputColumn(mt, column));
        if (mt.getFilter() != null) {
            checkTargetFilter(mt, mt.getFilter());
        }
    }

    private void checkTargetFilter(MvTarget mt, MvComputation filter) {
        for (var src : filter.getSources()) {
            if (src.getReference() != null
                    && src.getReference().getTableInfo() != null) {
                boolean exists = src.getReference().getTableInfo()
                        .getColumns().containsKey(src.getColumn());
                if (! exists) {
                    context.addIssue(new MvIssue.UnknownColumn(
                            mt, src.getAlias(), src.getColumn(), filter));
                }
            }
        }
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
            } else if (cond.getFirstAlias() != null && cond.getSecondAlias() == null
                    && cond.getFirstAlias().equals(cond.getSecondAlias())) {
                // TODO: maybe a different issue with better explanation
                context.addIssue(new MvIssue.IllegalJoinCondition(mt, src, cond));
            } else {
                checkJoinColumns(mt, cond);
            }
        }
    }

    private void checkJoinColumns(MvTarget mt, MvJoinCondition cond) {
        if (cond.getFirstAlias()!=null) {
            MvJoinSource ref = mt.getSourceByAlias(cond.getFirstAlias());
            if (ref!=null && ref.getTableInfo()!=null) {
                if (ref.getTableInfo().getColumns().get(cond.getFirstColumn())==null) {
                    context.addIssue(new MvIssue.UnknownColumnInCondition(
                            mt, cond, cond.getFirstAlias(), cond.getFirstColumn()));
                }
            }
        }
        if (cond.getSecondAlias()!=null) {
            MvJoinSource ref = mt.getSourceByAlias(cond.getSecondAlias());
            if (ref!=null && ref.getTableInfo()!=null) {
                if (ref.getTableInfo().getColumns().get(cond.getSecondColumn())==null) {
                    context.addIssue(new MvIssue.UnknownColumnInCondition(
                            mt, cond, cond.getSecondAlias(), cond.getSecondColumn()));
                }
            }
        }
    }

    private void checkTargetOutputColumn(MvTarget mt, MvColumn column) {
        if (column.isComputation()) {
            MvComputation comp = column.getComputation();
            for (var src : comp.getSources()) {
                if (src.getReference() != null
                        && src.getReference().getTableInfo() != null) {
                    boolean exists = src.getReference().getTableInfo()
                            .getColumns().containsKey(src.getColumn());
                    if (! exists) {
                        context.addIssue(new MvIssue.UnknownColumn(
                                mt, src.getAlias(), src.getColumn(), comp));
                    }
                }
            }
        } else {
            MvJoinSource src = mt.getSourceByAlias(column.getSourceAlias());
            if (src==null || src.getTableInfo()==null
                    || src.getTableInfo().getColumns().get(column.getSourceColumn())==null) {
                context.addIssue(new MvIssue.IllegalOutputReference(mt, column));
            }
            if (mt.getTableInfo()==null) {
                return;
            }
            if (mt.getTableInfo().getColumns().get(column.getName())==null) {
                context.addIssue(new MvIssue.UnknownOutputColumn(mt, column));
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
            List<String> joinColumns = src.collectRightJoinColumns();
            if (joinColumns.isEmpty()) {
                continue;
            }
            // Find the proper index
            String indexName = src.getTableInfo().findProperIndex(joinColumns);
            // If no covering index found, add warning
            if (indexName==null) {
                context.addIssue(new MvIssue.MissingJoinIndex(mt, src, joinColumns));
            }
        }
    }

    private void checkKeyExtractionIndexes(MvTarget mt) {
        MvPathGenerator pathGenerator = new MvPathGenerator(mt);
        for (int pos = 1; pos < mt.getSources().size(); ++pos) {
            MvJoinSource js = mt.getSources().get(pos);
            if (!js.isTableKnown() || js.getInput()==null) {
                continue;
            }
            if (js.getInput().isBatchMode()) {
                continue;
            }
            MvTarget temp = pathGenerator.extractKeysReverse(js);
            if (temp==null) {
                context.addIssue(new MvIssue.KeyExtractionImpossible(mt, js));
            } else {
                checkJoinIndexes(temp);
            }
        }
    }

    private void checkChangefeeds() {
        for (MvHandler mh : context.getHandlers().values()) {
            for (MvInput mi : mh.getInputs().values()) {
                checkChangefeed(mh, mi);
            }
        }
    }

    private void checkChangefeed(MvHandler mh, MvInput mi) {
        if (mi.getTableInfo() == null) {
            context.addIssue(new MvIssue.UnknownInputTable(mi));
        } else {
            if (mi.getChangefeed() == null
                    || mi.getTableInfo().getChangefeeds().get(mi.getChangefeed()) == null) {
                context.addIssue(new MvIssue.UnknownChangefeed(mi));
            } else if (mi.getChangefeedInfo() != null) {
                String desiredConsumer;
                if (mi.isBatchMode()) {
                    desiredConsumer = context.getDictionaryConsumer();
                } else {
                    desiredConsumer = mh.getConsumerNameAlways();
                }
                if (! mi.getChangefeedInfo().getConsumers().contains(desiredConsumer)) {
                    context.addIssue(new MvIssue.MissingConsumer(mi, desiredConsumer));
                }
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
                context.addIssue(new MvIssue.MissingInput(mh, mt, joinSource));
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
