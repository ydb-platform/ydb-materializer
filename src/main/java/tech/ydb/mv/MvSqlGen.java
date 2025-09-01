package tech.ydb.mv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvLiteral;
import tech.ydb.mv.model.MvTableInfo;

/**
 * SQL query generation logic.
 *
 * @author zinal
 */
public class MvSqlGen implements AutoCloseable {

    public static final String SYS_CONST = "sys_const";
    public static final String SYS_KEYS = "sys_keys";
    public static final String SYS_KEYS_VAR = "$sys_keys";
    public static final String SYS_INPUT = "sys_input";
    public static final String SYS_INPUT_VAR = "$sys_input";

    public static final Pattern SAFE_ID_PATT = Pattern.compile("^[A-Za-z][A-Za-z0-9_]*$");
    public static final String EOL = System.getProperty("line.separator");

    private final MvTarget target;

    public MvSqlGen(MvTarget target) {
        if (target==null) {
            throw new NullPointerException("target argument cannot be null");
        }
        this.target = target;
    }

    @Override
    public void close() {
        /* noop */
    }

    public StructType toKeyType() {
        return toKeyType(target);
    }

    public StructType toRowType() {
        return toRowType(target);
    }

    public String getMainTable() {
        return target.getTopMostSource().getTableInfo().getName();
    }

    public String makeCreateView() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE VIEW ");
        safeId(sb, target.getName()).append(EOL);
        sb.append("  WITH (security_invoker=TRUE) AS").append(EOL);
        genFullSelect(sb, false);
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makeSelect() {
        final StringBuilder sb = new StringBuilder();
        genDeclareMainKeyFields(sb);
        genFullSelect(sb, true);
        sb.append(";").append(EOL);
        return sb.toString();
    }

    @Deprecated
    public String makeUpsertSelect() {
        final StringBuilder sb = new StringBuilder();
        genDeclareMainKeyFields(sb);
        sb.append("UPSERT INTO ");
        safeId(sb, target.getName()).append(EOL);
        genFullSelect(sb, true);
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makePlainUpsert() {
        final StringBuilder sb = new StringBuilder();
        genDeclareTargetFields(sb);
        sb.append("UPSERT INTO ");
        safeId(sb, target.getName()).append(EOL);
        sb.append("SELECT * FROM AS_TABLE(").append(SYS_INPUT_VAR).append(")");
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makePlainDelete() {
        final StringBuilder sb = new StringBuilder();
        genDeclareTargetFields(sb);
        sb.append("DELETE FROM ");
        safeId(sb, target.getName()).append(EOL);
        sb.append(" ON SELECT * FROM AS_TABLE(").append(SYS_KEYS_VAR).append(")");
        sb.append(";").append(EOL);
        return sb.toString();
    }

    private void genDeclareMainKeyFields(StringBuilder sb) {
        if (target.getSources().isEmpty()) {
            throw new IllegalStateException("No source tables for target `" + target.getName() + "`");
        }
        sb.append("DECLARE ").append(SYS_KEYS_VAR).append(" AS ");
        sb.append("List<");
        formatType(sb, toKeyType());
        sb.append(">;").append(EOL);
    }

    private void genDeclareTargetFields(StringBuilder sb) {
        if (target.getTableInfo()==null) {
            throw new IllegalStateException("No table definition for target `" + target.getName() + "`");
        }
        sb.append("DECLARE ").append(SYS_INPUT_VAR).append(" AS ");
        sb.append("List<");
        structForTable(sb, target.getTableInfo());
        sb.append(">;").append(EOL);
    }

    private void genFullSelect(StringBuilder sb, boolean withInputKeys) {
        sb.append("SELECT").append(EOL);

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
            sb.append(EOL);
        }

        // Generate simple FROM/JOIN structure
        genSourcesPart(sb, withInputKeys);

        // Add WHERE clause if present
        if (target.getFilter() != null) {
            sb.append("WHERE ");
            genExpression(sb, target.getFilter());
            sb.append(EOL);
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
        sb.append("FROM (SELECT").append(EOL);
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
            sb.append(EOL);
        }
        sb.append(") AS ").append(SYS_CONST).append(EOL);
    }

    private void genInputKeys(StringBuilder sb) {
        sb.append("AS_TABLE(").append(SYS_KEYS_VAR)
                .append(") AS ").append(SYS_KEYS).append(EOL);
        sb.append("INNER JOIN ");
    }

    private void genInputCondition(StringBuilder sb) {
        if (target.getSources().isEmpty()) {
            throw new IllegalStateException("No source tables for target `" + target.getName() + "`");
        }
        var mainTable = target.getTopMostSource();
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
        sb.append(EOL);
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
        sb.append(EOL);
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
            sb.append(EOL);
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
        if (c.isLiteral()) {
            sb.append(c.getLiteral().getValue());
        } else {
            sb.append(c.getExpression());
        }
    }

    public static StructType toKeyType(MvTarget target) {
        if (target==null || target.getSources().isEmpty()) {
            throw new IllegalArgumentException();
        }
        return toKeyType(target.getTopMostSource().getTableInfo());
    }

    public static StructType toKeyType(MvTableInfo ti) {
        final HashMap<String, Type> m = new HashMap<>();
        for (String k : ti.getKey()) {
            m.put(k, ti.getColumns().get(k));
        }
        return StructType.of(m);
    }

    public static StructType toRowType(MvTarget target) {
        if (target==null || target.getTableInfo()==null) {
            throw new IllegalArgumentException();
        }
        return toRowType(target.getTableInfo());
    }

    public static StructType toRowType(MvTableInfo ti) {
        return StructType.of(ti.getColumns());
    }

    public static String formatType(Type t) {
        if (t==null) {
            throw new NullPointerException();
        }
        if (t instanceof StructType st) {
             return formatType(new StringBuilder(), st).toString();
        }
        return t.toString();
    }

    public static StringBuilder formatType(StringBuilder sb, StructType st) {
        if (st==null) {
            throw new NullPointerException();
        }
        if (sb==null) {
            sb = new StringBuilder();
        }
        sb.append("Struct<");
        for (int i=0; i<st.getMembersCount(); ++i) {
            String name = st.getMemberName(i);
            String type = MvSqlGen.formatType(st.getMemberType(i));
            if (i>0) {
                sb.append(",");
            }
            safeId(sb, name).append(":").append(type);
        }
        sb.append(">");
        return sb;
    }

    public static StringBuilder structForTable(StringBuilder sb, MvTableInfo ti) {
        if (ti==null) {
            throw new NullPointerException();
        }
        if (sb==null) {
            sb = new StringBuilder();
        }
        sb.append("Struct<");
        boolean comma = false;
        for (Map.Entry<String, Type> me : ti.getColumns().entrySet()) {
            String name = me.getKey();
            String type = MvSqlGen.formatType(me.getValue());
            if (comma) {
                sb.append(",");
            } else {
                comma = true;
            }
            safeId(sb, name).append(":").append(type);
        }
        sb.append(">");
        return sb;
    }

    public static String structForTable(MvTableInfo ti) {
        return structForTable(null, ti).toString();
    }

}
