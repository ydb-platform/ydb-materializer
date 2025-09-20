package tech.ydb.mv.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Status;

import tech.ydb.mv.YdbConnector;
import tech.ydb.table.Session;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvTarget;

/**
 * SQL validation for MV context - used to detect errors in opaque expressions.
 *
 * @author zinal
 */
public class MvValidateSql {

    private final MvMetadata context;
    private final YdbConnector conn;

    public MvValidateSql(MvMetadata context, YdbConnector conn) {
        this.context = context;
        this.conn = conn;
    }

    public boolean validate() {
        if (!context.isValid()) {
            return false;
        }
        for (MvTarget target : context.getTargets().values()) {
            validateTarget(target);
        }
        return context.isValid();
    }

    private boolean validateTarget(MvTarget target) {
        MvSqlGen sg = new MvSqlGen(target);
        // fast track - attempt to check the whole SELECT, if valid - stop
        String currentSql = sg.makeSelect();
        String originalIssues = validateSql(currentSql);
        if (originalIssues == null) {
            return true;
        }
        // there are issues with the whole SELECT, probably bad opaque expressions
        // trying to isolate and report properly
        HashSet<String> knownIssues = new HashSet<>();
        List<MvColumn> exprColumns = collectExpressionColumns(target);
        if (exprColumns.isEmpty() && target.getFilter() != null) {
            // no expressions in columns, and we have the filter - so it is wrong
            context.addIssue(new MvIssue.SqlCustomFilterError(target, target.getFilter(), originalIssues));
            return false;
        }
        // check the filter
        if (target.getFilter() != null) {
            validateFilter(target, sg, exprColumns, knownIssues);
        }
        if (exprColumns.size() > 1) {
            for (MvColumn current : exprColumns) {
                // safe placeholders for all but the current column
                // safe placeholder for WHERE filter
                validateColumn(target, current, sg, exprColumns, knownIssues);
            }
        } else if (exprColumns.size() == 1) {
            // single expression column
            MvColumn current = exprColumns.iterator().next();
            validateColumn(target, current, sg, exprColumns, knownIssues);
        }
        if (knownIssues.isEmpty()) {
            // Could not localize the error, but still need to report it.
            context.addIssue(new MvIssue.SqlUnexpectedError(target, originalIssues));
        }
        return false;
    }

    private void validateFilter(MvTarget target, MvSqlGen sg,
            List<MvColumn> exprColumns, HashSet<String> knownIssues) {
        // safe placeholders for all columns
        maskAllExcept(null, exprColumns, sg);
        // now checking the filter
        String currentSql = sg.makeSelect();
        String issues = validateSql(currentSql);
        if (issues != null && knownIssues.add(issues)) {
            context.addIssue(new MvIssue.SqlCustomFilterError(target, target.getFilter(), issues));
        }
    }

    private void validateColumn(MvTarget target, MvColumn current, MvSqlGen sg,
            List<MvColumn> exprColumns, HashSet<String> knownIssues) {
        maskAllExcept(current, exprColumns, sg);
        if (target.getFilter() != null) {
            // safe placeholder in WHERE
            sg.getExcludedComputations().add(target.getFilter());
        }
        String currentSql = sg.makeSelect();
        String issues = validateSql(currentSql);
        if (issues != null && knownIssues.add(issues)) {
            context.addIssue(new MvIssue.SqlCustomColumnError(target, current, issues));
        }
    }

    private String validateSql(String sql) {
        Status status = conn.getTableRetryCtx().supplyStatus(
                sess -> validateSql(sess, sql))
                .join();
        return extractErrors(status);
    }

    private CompletableFuture<Status> validateSql(Session sess, String sql) {
        return sess.prepareDataQuery(sql).thenApply(result -> result.getStatus());
    }

    private String extractErrors(Status status) {
        if (status.isSuccess()) {
            return null;
        }
        return status.toString();
    }

    private List<MvColumn> collectExpressionColumns(MvTarget target) {
        List<MvColumn> output = new ArrayList<>();
        for (MvColumn c : target.getColumns()) {
            if (c.isComputation() && !c.getComputation().isLiteral()) {
                output.add(c);
            }
        }
        return output;
    }

    private void maskAllExcept(MvColumn exclude, List<MvColumn> exprColumns, MvSqlGen sg) {
        sg.getExcludedComputations().clear();
        for (MvColumn column : exprColumns) {
            if (column == exclude) {
                continue;
            }
            sg.getExcludedComputations().add(column.getComputation());
        }
        // TODO: debugging code, remove
        if (exclude != null) {
            if (sg.getExcludedComputations().contains(exclude.getComputation())) {
                throw new IllegalStateException("Internal error, current column expression got excluded");
            }
        }
        for (MvColumn column : exprColumns) {
            if (column == exclude) {
                continue;
            }
            if (!sg.getExcludedComputations().contains(column.getComputation())) {
                throw new IllegalStateException("Internal error, other column expression got included");
            }
        }
    }

}
