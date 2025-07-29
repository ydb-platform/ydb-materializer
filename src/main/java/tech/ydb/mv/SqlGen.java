package tech.ydb.mv;

import java.util.ArrayList;
import java.util.regex.Pattern;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvLiteral;
import tech.ydb.mv.model.MvJoinMode;

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
        sb.append("CREATE VIEW ");
        safeId(sb, target.getName()).append(eol);
        sb.append("  WITH (security_invoker=TRUE) AS").append(eol);
        sb.append("SELECT").append(eol);

        // Generate column list
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

        // Check if we have any literals in join conditions
        boolean hasLiterals = hasLiteralsInJoins();

        if (hasLiterals) {
            // Generate constants subquery and CROSS JOIN
            genFromWithConstants(sb);
        } else {
            // Generate simple FROM/JOIN structure
            genFromSimple(sb);
        }

        // Add WHERE clause if present
        if (target.getFilter() != null) {
            sb.append("WHERE ");
            genExpression(sb, target.getFilter());
            sb.append(eol);
        }

        sb.append(";").append(eol);
        return sb.toString();
    }

    private boolean hasLiteralsInJoins() {
        return !target.getLiterals().isEmpty();
    }

    private void genFromWithConstants(StringBuilder sb) {
        // Start with constants subquery
        genConstantsSubquery(sb);

        // Add CROSS JOIN with main table and proper joins with other tables
        boolean firstJoin = true;
        for (MvJoinSource source : target.getSources()) {
            if (source.getMode() == MvJoinMode.MAIN) {
                if (firstJoin) {
                    sb.append("CROSS JOIN ");
                    firstJoin = false;
                } else {
                    // Add join type
                    switch (source.getMode()) {
                        case INNER ->
                            sb.append("INNER JOIN ");
                        case LEFT ->
                            sb.append("LEFT JOIN ");
                        default ->
                            throw new IllegalStateException("Unsupported join mode: " + source.getMode());
                    }
                }
                genJoinTable(sb, source);
            }
        }

        // Add other joins
        for (MvJoinSource source : target.getSources()) {
            if (source.getMode() != MvJoinMode.MAIN) {
                genJoinSource(sb, source);
            }
        }
    }

    private void genFromSimple(StringBuilder sb) {
        boolean firstJoin = true;
        for (MvJoinSource source : target.getSources()) {
            if (firstJoin) {
                sb.append("FROM ");
                firstJoin = false;
            } else {
                genJoinSource(sb, source);
            }
            genJoinTable(sb, source);
        }
    }

    private void genConstantsSubquery(StringBuilder sb) {
        sb.append("FROM (SELECT").append(eol);
        boolean comma = false;
        for (MvLiteral literal : target.getLiterals()) {
            if (comma) {
                sb.append("  ,");
            } else {
                sb.append("   ");
                comma = true;
            }
            sb.append(literal.getValue()).append(" AS ");
            safeId(sb, literal.getIdentity());
            sb.append(eol);
        }
        sb.append(") AS constants").append(eol);
    }

    private void genJoinSource(StringBuilder sb, MvJoinSource source) {
        // Add join type
        switch (source.getMode()) {
            case INNER ->
                sb.append("INNER JOIN ");
            case LEFT ->
                sb.append("LEFT JOIN ");
            default ->
                throw new IllegalStateException("Unsupported join mode: " + source.getMode());
        }

        genJoinTable(sb, source);
    }

    private void genJoinTable(StringBuilder sb, MvJoinSource source) {
        // Add table name and alias
        safeId(sb, source.getTableName());
        sb.append(" AS ");
        safeId(sb, source.getTableAlias());
        sb.append(eol);

        // Add ON conditions
        genJoinConditions(sb, source.getConditions());
    }

    private void genJoinConditions(StringBuilder sb, ArrayList<MvJoinCondition> conditions) {
        if (!conditions.isEmpty()) {
            sb.append("    ON ");
            boolean firstCondition = true;
            for (MvJoinCondition condition : conditions) {
                if (!firstCondition) {
                    sb.append(" AND ");
                }
                genJoinCondition(sb, condition);
                firstCondition = false;
            }
            sb.append(eol);
        }
    }

    private void genJoinCondition(StringBuilder sb, MvJoinCondition condition) {
        // First side of the condition
        if (condition.getFirstLiteral() != null) {
            sb.append("constants.");
            safeId(sb, condition.getFirstLiteral().getIdentity());
        } else {
            safeId(sb, condition.getFirstAlias());
            sb.append(".");
            safeId(sb, condition.getFirstColumn());
        }

        sb.append(" = ");

        // Second side of the condition
        if (condition.getSecondLiteral() != null) {
            sb.append("constants.");
            safeId(sb, condition.getSecondLiteral().getIdentity());
        } else {
            safeId(sb, condition.getSecondAlias());
            sb.append(".");
            safeId(sb, condition.getSecondColumn());
        }
    }

    private StringBuilder safeId(StringBuilder sb, String identifier) {
        if (safeIdPattern.matcher(identifier).matches()) {
            sb.append(identifier);
        } else if (identifier.startsWith("`") && identifier.endsWith("`")) {
            // Already quoted identifier, preserve as is
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
        sb.append(" AS ");
        safeId(sb, c.getName());
    }

    private void genExpression(StringBuilder sb, MvComputation c) {
        sb.append(c.getExpression());
    }

}
