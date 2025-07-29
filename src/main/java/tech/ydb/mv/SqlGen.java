package tech.ydb.mv;

import java.util.regex.Pattern;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author mzinal
 */
public class SqlGen {

    private final MvTarget target;
    private final Pattern safeIdPattern = Pattern.compile("^[A-Za-z][A-Za-z0-9_]*$");
    private final String eol = System.getProperty("line.separator");

    public SqlGen(MvTarget target) {
        this.target = target;
    }

    public String makeCreateView() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE VIEW "); safeId(sb, target.getName()).append(eol);
        sb.append("  WITH (security_invoker=TRUE) AS").append(eol);
        sb.append("SELECT").append(eol);
        boolean comma = false;
        for (MvColumn c : target.getColumns()) {
            if (comma) {
                sb.append("  ,");
            } else {
                sb.append("   ");
                comma = true;
            }
            genColumn(sb, c);
            sb.append(eol);
        }
        for (MvJoinSource j : target.getSources()) {
            genJoin(sb, j);
        }
        sb.append(";").append(eol);
        return sb.toString();
    }

    private StringBuilder safeId(StringBuilder sb, String identifier) {
        if (safeIdPattern.matcher(identifier).matches()) {
            sb.append(identifier);
        } else {
            sb.append("`").append(identifier.replace('`', '_')).append("`");
        }
        return sb;
    }

    private void genColumn(StringBuilder sb, MvColumn c) {
        if (c.isComputation()) {
            genExpression(sb, c.getComputation());
        } else {
            safeId(sb, c.getSourceAlias());
            sb.append(".");
            safeId(sb, c.getSourceColumn());
        }
        sb.append(" AS ").append("`").append(c.getName()).append("`");
    }

    private void genExpression(StringBuilder sb, MvComputation c) {
        sb.append(c.getExpression());
    }

    private void genJoin(StringBuilder sb, MvJoinSource j) {
        switch (j.getMode()) {
            case MAIN -> { sb.append("FROM "); }
            case INNER -> { sb.append("INNER JOIN "); }
            case LEFT -> { sb.append("LEFT JOIN "); }
            default -> { throw new IllegalStateException(); }
        }
        safeId(sb, j.getTableName());
    }


}
