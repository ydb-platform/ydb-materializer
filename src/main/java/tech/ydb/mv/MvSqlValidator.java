package tech.ydb.mv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import tech.ydb.core.Status;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.table.Session;

import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.parser.MvSqlGen;

/**
 * SQL validation for MV context - used to detect errors in opaque expressions.
 *
 * @author zinal
 */
public class MvSqlValidator {

    private final MvContext context;
    private final YdbConnector conn;

    public MvSqlValidator(MvContext context, YdbConnector conn) {
        this.context = context;
        this.conn = conn;
    }

    public boolean validate() {
        if (! context.isValid()) {
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
        String largeSql = sg.makeSelect();
        if (validateSql(largeSql) == null) {
            return true;
        }
        // there are issues with the whole SELECT, probably bad opaque expressions
        // trying to isolate and report properly
        HashSet<String> knownIssues = new HashSet<>();
        MvComputation filter = target.getFilter();
        List<MvColumn> exprColumns = collectExpressionColumns(target);
        for (int index = 0; index < exprColumns.size(); ++index) {
            MvColumn column = exprColumns.get(index);
            maskAllExcept(index, exprColumns, sg);
            if (filter != null) {
                sg.getExcludedComputations().add(filter);
            }
            String stepSql = sg.makeSelect();
            String issues = validateSql(stepSql);
            if (issues != null && knownIssues.add(issues)) {
                context.addIssue(new MvIssue.SqlCustomColumnError(target, column, issues));
            }
        }
        if (filter!=null) {
            maskAllExcept(-1, exprColumns, sg);
            String stepSql = sg.makeSelect();
            String issues = validateSql(stepSql);
            if (issues != null && knownIssues.add(issues)) {
                context.addIssue(new MvIssue.SqlCustomFilterError(target, filter, issues));
            }
        }
        return false;
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

    private void maskAllExcept(int index, List<MvColumn> exprColumns, MvSqlGen sg) {
        sg.getExcludedComputations().clear();
        for (int i = 0; i < exprColumns.size(); ++i) {
            if (i == index) {
                continue;
            }
            MvColumn c = exprColumns.get(0);
            sg.getExcludedComputations().add(c.getComputation());
        }
    }

}
