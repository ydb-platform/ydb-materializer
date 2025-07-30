package tech.ydb.mv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvLiteral;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 * SQL query generation logic.
 * @author mzinal
 */
public class SqlGen {

    public static final String SYS_CONST = "sys_const";
    public static final String SYS_KEYS = "sys_keys";

    private static final Pattern SAFE_ID_PATT = Pattern.compile("^[A-Za-z][A-Za-z0-9_]*$");

    private final MvTarget target;
    private final String eol = System.getProperty("line.separator");

    public SqlGen(MvTarget target) {
        this.target = target;
    }

    public String makeCreateView() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE VIEW ");
        safeId(sb, target.getName()).append(eol);
        sb.append("  WITH (security_invoker=TRUE) AS").append(eol);
        genFullSelect(sb, false);
        sb.append(";").append(eol);
        return sb.toString();
    }

    public String makeSelect() {
        final StringBuilder sb = new StringBuilder();
        genDeclareMainKeys(sb);
        genFullSelect(sb, true);
        sb.append(";").append(eol);
        return sb.toString();
    }

    public String makeUpsert() {
        final StringBuilder sb = new StringBuilder();
        genDeclareMainKeys(sb);
        sb.append("UPSERT INTO ");
        safeId(sb, target.getName()).append(eol);
        genFullSelect(sb, true);
        sb.append(";").append(eol);
        return sb.toString();
    }

    private void genDeclareMainKeys(StringBuilder sb) {
        if (target.getSources().isEmpty()) {
            throw new IllegalStateException("No source tables for target `" + target.getName() + "`");
        }
        var keyType = toKeyType(target.getSources().get(0).getTableInfo());
        sb.append("DECLARE $").append(SYS_KEYS).append(" AS ");
        sb.append("List<");
        typeToString(sb, keyType);
        sb.append(">;").append(eol);
    }

    private void genFullSelect(StringBuilder sb, boolean withInputKeys) {
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

        // Generate simple FROM/JOIN structure
        genSourcesPart(sb, withInputKeys);

        // Add WHERE clause if present
        if (target.getFilter() != null) {
            sb.append("WHERE ");
            genExpression(sb, target.getFilter());
            sb.append(eol);
        }
    }

    private boolean hasLiteralsInJoins() {
        return !target.getLiterals().isEmpty();
    }

    private void genSourcesPart(StringBuilder sb, boolean withInputKeys) {
        // Check if we have any literals in join conditions
        boolean withConstants = hasLiteralsInJoins();
        String mainTableStatement = "FROM";
        if (withConstants) {
            // Start with constants subquery
            genConstantsSubquery(sb);
            mainTableStatement = "CROSS JOIN";
        }

        // Add main table and proper joins with other tables
        boolean firstJoin = true;
        for (MvJoinSource source : target.getSources()) {
            if (firstJoin) {
                sb.append(mainTableStatement).append(" ");
                if (withInputKeys) {
                    // AS_TABLE($sys_keys) AS sys_keys INNER JOIN
                    genInputKeys(sb);
                }
            } else {
                // Add join type
                genJoinSource(sb, source);
            }
            // tableName AS tableAlias
            genJoinTable(sb, source);
            if (firstJoin) {
                if (withInputKeys) {
                    genInputCondition(sb);
                }
                firstJoin = false;
            } else {
                genJoinConditions(sb, source.getConditions());
            }
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
        sb.append(") AS ").append(SYS_CONST).append(eol);
    }

    private void genInputKeys(StringBuilder sb) {
        sb.append("AS_TABLE($").append(SYS_KEYS)
                .append(") AS ").append(SYS_KEYS).append(eol);
        sb.append("INNER JOIN ");
    }

    private void genInputCondition(StringBuilder sb) {
        if (target.getSources().isEmpty()) {
            throw new IllegalStateException("No source tables for target `" + target.getName() + "`");
        }
        var mainTable = target.getSources().get(0);
        var primaryKey = mainTable.getTableInfo().getKey();
        String statement = "    ON ";
        for (String pk : primaryKey) {
            sb.append(statement);
            statement = " AND ";
            sb.append(SYS_KEYS).append(".");
            safeId(sb, pk).append(" = ");
            safeId(sb, mainTable.getTableAlias()).append(".");
            safeId(sb, pk);
        }
        sb.append(eol);
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
    }

    private void genJoinTable(StringBuilder sb, MvJoinSource source) {
        // Add table name and alias
        safeId(sb, source.getTableName());
        sb.append(" AS ");
        safeId(sb, source.getTableAlias());
        sb.append(eol);
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
            sb.append(SYS_CONST).append(".");
            safeId(sb, condition.getFirstLiteral().getIdentity());
        } else {
            safeId(sb, condition.getFirstAlias());
            sb.append(".");
            safeId(sb, condition.getFirstColumn());
        }

        sb.append(" = ");

        // Second side of the condition
        if (condition.getSecondLiteral() != null) {
            sb.append(SYS_CONST).append(".");
            safeId(sb, condition.getSecondLiteral().getIdentity());
        } else {
            safeId(sb, condition.getSecondAlias());
            sb.append(".");
            safeId(sb, condition.getSecondColumn());
        }
    }

    public static StringBuilder safeId(StringBuilder sb, String identifier) {
        if (SAFE_ID_PATT.matcher(identifier).matches()) {
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

    public static StructType toKeyType(MvTableInfo ti) {
        final HashMap<String, Type> m = new HashMap<>();
        for (String k : ti.getKey()) {
            m.put(k, ti.getColumns().get(k));
        }
        return StructType.of(m);
    }

    public static StringBuilder typeToString(StringBuilder sb, StructType st) {
        if (st==null) {
            throw new NullPointerException();
        }
        sb.append("Struct<");
        for (int i=0; i<st.getMembersCount(); ++i) {
            String name = st.getMemberName(i);
            String type = typeToString(st.getMemberType(i));
            if (i>0) {
                sb.append(",");
            }
            safeId(sb, name).append(":").append(type);
        }
        sb.append(">");
        return sb;
    }

    public static String typeToString(Type t) {
        if (t==null) {
            throw new NullPointerException();
        }
        if (t instanceof StructType st) {
             return typeToString(new StringBuilder(), st).toString();
        }
        return t.toString();
    }

}
