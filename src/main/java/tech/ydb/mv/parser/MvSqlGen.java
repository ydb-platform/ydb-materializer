package tech.ydb.mv.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvViewExpr;
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

    private final MvViewExpr target;
    private final HashSet<MvComputation> excludedComputations;

    public MvSqlGen(MvViewExpr target) {
        if (target == null) {
            throw new NullPointerException("target argument cannot be null");
        }
        this.target = target;
        this.excludedComputations = new HashSet<>();
    }

    public MvViewExpr getTarget() {
        return target;
    }

    public HashSet<MvComputation> getExcludedComputations() {
        return excludedComputations;
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

    /**
     * The create table variant grabs data types from the input tables, and
     * combines them into the definition of the output MV table.
     *
     * @return CREATE TABLE statement
     */
    public String makeCreateTable() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        safeId(sb, target.getName());
        sb.append(" (").append(EOL);
        int index = 0;
        for (MvColumn column : target.getColumns()) {
            if (index++ > 0) {
                sb.append(",").append(EOL);
            }
            sb.append("  ");
            safeId(sb, column.getName());
            sb.append(" ");
            Type type;
            if (column.isComputation()) {
                type = detectComputationType(column.getComputation()).makeOptional();
            } else {
                type = obtainReferenceType(column);
            }
            if (type.getKind() == Type.Kind.OPTIONAL) {
                sb.append(type.unwrapOptional().toString());
            } else {
                sb.append(type.toString()).append(" NOT NULL");
            }
        }
        ArrayList<String> primaryKey = findMappedKeyColumns();
        if (primaryKey != null && !primaryKey.isEmpty()) {
            if (index++ > 0) {
                sb.append(",").append(EOL);
            }
            sb.append("  PRIMARY KEY (");
            int index2 = 0;
            for (String name : primaryKey) {
                if (index2++ > 0) {
                    sb.append(", ");
                }
                safeId(sb, name);
            }
            sb.append(")").append(EOL);
        }
        sb.append(");").append(EOL);
        return sb.toString();
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

    public String makeSelectAll() {
        final StringBuilder sb = new StringBuilder();
        genFullSelect(sb, false);
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makeSelect() {
        final StringBuilder sb = new StringBuilder();
        genDeclareMainKeyList(sb);
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
        genDeclareMainKeyList(sb);
        sb.append("DELETE FROM ");
        safeId(sb, target.getName()).append(EOL);
        if (canDeleteByInputKeys()) {
            sb.append(" ON SELECT * FROM AS_TABLE(").append(SYS_KEYS_VAR).append(")");
        } else if (canDeleteKeysFromInput()) {
            sb.append(" ON SELECT").append(EOL);
            genDeleteKeySelectList(sb);
            sb.append("FROM AS_TABLE(").append(SYS_KEYS_VAR).append(") AS ");
            safeId(sb, target.getTopMostSource().getTableAlias()).append(EOL);
        } else {
            throw new IllegalStateException("Delete keys require source-side computation");
        }
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makePlainDeleteByTargetKeys() {
        final StringBuilder sb = new StringBuilder();
        genDeclareTargetKeyList(sb);
        sb.append("DELETE FROM ");
        safeId(sb, target.getName()).append(EOL);
        sb.append(" ON SELECT * FROM AS_TABLE(").append(SYS_KEYS_VAR).append(")");
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makeSelectDeleteKeys() {
        final StringBuilder sb = new StringBuilder();
        genDeclareMainKeyList(sb);
        sb.append("SELECT").append(EOL);
        genTargetKeySelectList(sb);
        genSourcesPart(sb, true);
        if (target.getFilter() != null) {
            sb.append("WHERE ");
            genExpression(sb, target.getFilter(), PrimitiveType.Bool);
            sb.append(EOL);
        }
        sb.append(";").append(EOL);
        return sb.toString();
    }

    public String makeScanNext() {
        MvTableInfo topmost = target.getTopMostSource().getTableInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $limit AS Uint64;").append(MvSqlGen.EOL);
        int index = 0;
        for (String name : topmost.getKey()) {
            sb.append("DECLARE $c").append(++index).append(" AS ");
            sb.append(topmost.getColumns().get(name));
            sb.append(";").append(MvSqlGen.EOL);
        }
        sb.append("SELECT ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("FROM `").append(topmost.getName()).append("`");
        sb.append(MvSqlGen.EOL);
        sb.append("WHERE (");
        keyNamesByComma(sb, topmost);
        sb.append(") > (");
        index = 0;
        for (int i = 0; i < topmost.getKey().size(); ++i) {
            if (index++ > 0) {
                sb.append(", ");
            }
            sb.append("$c").append(index);
        }
        sb.append(")").append(MvSqlGen.EOL);
        sb.append("ORDER BY ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("LIMIT $limit;");
        sb.append(MvSqlGen.EOL);
        return sb.toString();
    }

    public String makeScanStart() {
        MvTableInfo topmost = target.getTopMostSource().getTableInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $limit AS Uint64;").append(MvSqlGen.EOL);
        sb.append("SELECT ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("FROM `").append(topmost.getName()).append("`");
        sb.append(MvSqlGen.EOL);
        sb.append("ORDER BY ");
        keyNamesByComma(sb, topmost);
        sb.append(MvSqlGen.EOL);
        sb.append("LIMIT $limit;");
        sb.append(MvSqlGen.EOL);
        return sb.toString();
    }

    private static void keyNamesByComma(StringBuilder sb, MvTableInfo topmost) {
        int index = 0;
        for (String name : topmost.getKey()) {
            if (index++ > 0) {
                sb.append(", ");
            }
            safeId(sb, name);
        }
    }

    private void genDeclareMainKeyList(StringBuilder sb) {
        if (target.getSources().isEmpty()) {
            throw new IllegalStateException("No source tables for target `" + target.getName() + "`");
        }
        sb.append("DECLARE ").append(SYS_KEYS_VAR).append(" AS ");
        sb.append("List<");
        formatType(sb, toKeyType());
        sb.append(">;").append(EOL);
    }

    private void genDeclareTargetFields(StringBuilder sb) {
        if (target.getTableInfo() == null) {
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
            genExpression(sb, target.getFilter(), PrimitiveType.Bool);
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
                genJoinType(sb, source);
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
            sb.append(literal.getSafeValue()).append(" AS ");
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

    private void genJoinType(StringBuilder sb, MvJoinSource source) {
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

    /**
     * Add table name and alias.
     */
    private void genJoinTable(StringBuilder sb, MvJoinSource source) {
        safeId(sb, source.getTableName());
        switch (source.getMode()) {
            case INNER:
            case LEFT: {
                String ixName = source.getTableInfo().findProperIndex(
                        source.collectRightJoinColumns());
                if (ixName != null) {
                    if (ixName.equals(MvTableInfo.PK_INDEX)) {
                        sb.append(" VIEW PRIMARY KEY");
                    } else {
                        sb.append(" VIEW ");
                        safeId(sb, ixName);
                    }
                }
                break;
            }
            case MAIN: {
                sb.append(" VIEW PRIMARY KEY");
                break;
            }
        }
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
            genExpression(sb, c.getComputation(), c.getType());
        } else {
            safeId(sb, c.getSourceAlias());
            sb.append(".");
            safeId(sb, c.getSourceColumn());
        }
        sb.append(" AS ");
        safeId(sb, c.getName());
    }

    private void genExpression(StringBuilder sb, MvComputation c, Type type) {
        if (c.isLiteral()) {
            sb.append(c.getLiteral().getSafeValue());
        } else {
            if (excludedComputations.contains(c)) {
                genExpressionPlaceholder(sb, type);
            } else {
                sb.append(c.getExpression());
            }
        }
    }

    private void genExpressionPlaceholder(StringBuilder sb, Type type) {
        if (type == null) {
            sb.append("CAST(NULL AS Text?)");
            return;
        }
        while (type.getKind() == Type.Kind.OPTIONAL) {
            type = type.unwrapOptional();
        }
        if (type.getKind() == Type.Kind.PRIMITIVE) {
            switch ((PrimitiveType) type) {
                case Bool:
                    sb.append("true");
                    break;
                case Text:
                    sb.append("''u");
                    break;
                case Bytes:
                    sb.append("''");
                    break;
                case Int8:
                case Int16:
                case Int32:
                case Int64:
                case Uint8:
                case Uint16:
                case Uint32:
                case Uint64:
                    sb.append("CAST(0 AS ").append(type.toString()).append("?)");
                    break;
                default:
                    sb.append("CAST(NULL AS ").append(type.toString()).append("?)");
            }
        } else {
            sb.append("CAST(NULL AS ").append(type.toString()).append("?)");
        }
    }

    private Type detectComputationType(MvComputation comp) {
        if (comp == null || comp.isEmpty()) {
            return PrimitiveType.Text;
        }
        if (comp.isLiteral()) {
            if (comp.getLiteral().isInteger()) {
                return PrimitiveType.Int64;
            }
            return PrimitiveType.Text;
        }
        String expr = comp.getExpression().toLowerCase();
        if (expr.lastIndexOf("decimal") >= 0) {
            return DecimalType.getDefault();
        }
        for (PrimitiveType t : PrimitiveType.values()) {
            if (expr.lastIndexOf(t.name().toLowerCase()) >= 0) {
                return t;
            }
        }
        return PrimitiveType.Text;
    }

    private Type obtainReferenceType(MvColumn column) {
        if (column == null || column.getSourceAlias() == null) {
            return PrimitiveType.Text;
        }
        MvJoinSource src = target.getSourceByAlias(column.getSourceAlias());
        if (src == null || src.getTableInfo() == null) {
            return PrimitiveType.Text;
        }
        return src.getTableInfo().getColumns().get(column.getSourceColumn());
    }

    private ArrayList<String> findMappedKeyColumns() {
        if (target.getTableInfo() != null && !target.getTableInfo().getKey().isEmpty()) {
            return new ArrayList<>(target.getTableInfo().getKey());
        }
        MvJoinSource topmost = target.getTopMostSource();
        if (topmost == null || topmost.getTableInfo() == null) {
            return null;
        }
        ArrayList<String> output = new ArrayList<>(topmost.getTableInfo().getKey().size());
        for (String name : topmost.getTableInfo().getKey()) {
            for (MvColumn column : target.getColumns()) {
                if (column.isReference()
                        && column.getSourceAlias().equals(topmost.getTableAlias())
                        && column.getSourceColumn().equals(name)) {
                    output.add(column.getName());
                    break;
                }
            }
        }
        return output;
    }

    public boolean canDeleteByInputKeys() {
        ArrayList<MvColumn> keyColumns = resolveTargetKeyColumns();
        MvJoinSource topmost = target.getTopMostSource();
        List<String> inputKeys = topmost.getTableInfo().getKey();
        if (keyColumns.size() != inputKeys.size()) {
            return false;
        }
        for (int i = 0; i < inputKeys.size(); ++i) {
            MvColumn keyColumn = keyColumns.get(i);
            String inputKey = inputKeys.get(i);
            if (!keyColumn.isReference()) {
                return false;
            }
            if (!inputKey.equals(keyColumn.getName())) {
                return false;
            }
            if (!topmost.getTableAlias().equals(keyColumn.getSourceAlias())) {
                return false;
            }
            if (!inputKey.equals(keyColumn.getSourceColumn())) {
                return false;
            }
        }
        return true;
    }

    public boolean canDeleteKeysFromInput() {
        ArrayList<MvColumn> keyColumns = resolveTargetKeyColumns();
        for (MvColumn keyColumn : keyColumns) {
            if (keyColumn.isComputation()) {
                if (!isDeleteComputationFromInput(keyColumn.getComputation())) {
                    return false;
                }
            } else if (keyColumn.isReference()) {
                if (!isDeleteKeyFromInput(keyColumn)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private void genDeleteKeySelectList(StringBuilder sb) {
        ArrayList<MvColumn> keyColumns = resolveTargetKeyColumns();
        boolean comma = false;
        for (MvColumn column : keyColumns) {
            if (comma) {
                sb.append("  ,");
            } else {
                sb.append("   ");
                comma = true;
            }
            genDeleteKeyExpression(sb, column);
            sb.append(" AS ");
            safeId(sb, column.getName());
            sb.append(EOL);
        }
    }

    private void genTargetKeySelectList(StringBuilder sb) {
        ArrayList<MvColumn> keyColumns = resolveTargetKeyColumns();
        boolean comma = false;
        for (MvColumn column : keyColumns) {
            if (comma) {
                sb.append("  ,");
            } else {
                sb.append("   ");
                comma = true;
            }
            genColumn(sb, column);
            sb.append(EOL);
        }
    }

    private void genDeleteKeyExpression(StringBuilder sb, MvColumn column) {
        if (column.isComputation()) {
            MvComputation comp = column.getComputation();
            if (comp != null && comp.isLiteral()) {
                sb.append(comp.getLiteral().getSafeValue());
            } else if (comp != null) {
                if (!isDeleteComputationFromInput(comp)) {
                    throw new IllegalStateException("Delete keys depend on unsupported computation: "
                            + column.getName());
                }
                sb.append(comp.getExpression());
            } else {
                genExpressionPlaceholder(sb, column.getType());
            }
            return;
        }
        if (!column.isReference()) {
            genExpressionPlaceholder(sb, column.getType());
            return;
        }
        if (!isDeleteKeyFromInput(column)) {
            throw new IllegalStateException("Delete keys depend on unsupported columns: "
                    + column.getName());
        }
        safeId(sb, column.getSourceAlias());
        sb.append(".");
        safeId(sb, column.getSourceColumn());
    }

    private void genDeclareTargetKeyList(StringBuilder sb) {
        sb.append("DECLARE ").append(SYS_KEYS_VAR).append(" AS ");
        sb.append("List<");
        formatType(sb, toTargetKeyType());
        sb.append(">;").append(EOL);
    }

    private StructType toTargetKeyType() {
        if (target.getTableInfo() != null && !target.getTableInfo().getKey().isEmpty()) {
            return toKeyType(target.getTableInfo());
        }
        ArrayList<MvColumn> keyColumns = resolveTargetKeyColumns();
        HashMap<String, Type> m = new HashMap<>();
        for (MvColumn column : keyColumns) {
            if (column.getType() == null) {
                throw new IllegalStateException("Missing key type for " + column.getName());
            }
            m.put(column.getName(), column.getType());
        }
        return StructType.of(m);
    }

    private boolean isDeleteKeyFromInput(MvColumn column) {
        if (!column.isReference()) {
            return false;
        }
        MvJoinSource topmost = target.getTopMostSource();
        if (!topmost.getTableAlias().equals(column.getSourceAlias())) {
            return false;
        }
        return topmost.getTableInfo().getKey().contains(column.getSourceColumn());
    }

    private boolean isDeleteComputationFromInput(MvComputation comp) {
        if (comp == null) {
            return false;
        }
        if (comp.isLiteral() || comp.getSources().isEmpty()) {
            return true;
        }
        MvJoinSource topmost = target.getTopMostSource();
        for (var src : comp.getSources()) {
            if (!topmost.getTableAlias().equals(src.getAlias())) {
                return false;
            }
            if (!topmost.getTableInfo().getKey().contains(src.getColumn())) {
                return false;
            }
        }
        return true;
    }

    private ArrayList<MvColumn> resolveTargetKeyColumns() {
        ArrayList<MvColumn> output = new ArrayList<>();
        if (target.getTableInfo() != null && !target.getTableInfo().getKey().isEmpty()) {
            for (String keyName : target.getTableInfo().getKey()) {
                MvColumn column = findColumnByName(keyName);
                if (column == null) {
                    return buildFallbackKeyColumns();
                }
                output.add(column);
            }
            return output;
        }
        return buildFallbackKeyColumns();
    }

    private MvColumn findColumnByName(String name) {
        for (MvColumn column : target.getColumns()) {
            if (name.equals(column.getName())) {
                return column;
            }
        }
        return null;
    }

    private ArrayList<MvColumn> buildFallbackKeyColumns() {
        ArrayList<MvColumn> output = new ArrayList<>();
        MvJoinSource topmost = target.getTopMostSource();
        for (String keyName : topmost.getTableInfo().getKey()) {
            MvColumn column = new MvColumn(keyName);
            column.setSourceAlias(topmost.getTableAlias());
            column.setSourceColumn(keyName);
            column.setSourceRef(topmost);
            column.setType(topmost.getTableInfo().getColumns().get(keyName));
            output.add(column);
        }
        return output;
    }

    public static StructType toKeyType(MvViewExpr target) {
        if (target == null || target.getSources().isEmpty()) {
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

    public static StructType toRowType(MvViewExpr target) {
        if (target == null) {
            throw new NullPointerException();
        }
        if (target.getColumns().isEmpty()) {
            throw new IllegalArgumentException("Passed target without column info");
        }
        HashMap<String, Type> columns = new HashMap<>();
        target.getColumns().forEach(c -> columns.put(c.getName(), c.getType()));
        return StructType.of(columns);
    }

    public static StructType toRowType(MvTableInfo ti) {
        return StructType.of(ti.getColumns());
    }

    public static String formatType(Type t) {
        if (t == null) {
            throw new NullPointerException();
        }
        if (t instanceof StructType st) {
            return formatType(new StringBuilder(), st).toString();
        }
        return t.toString();
    }

    public static StringBuilder formatType(StringBuilder sb, StructType st) {
        if (st == null) {
            throw new NullPointerException();
        }
        if (sb == null) {
            sb = new StringBuilder();
        }
        sb.append("Struct<");
        for (int i = 0; i < st.getMembersCount(); ++i) {
            String name = st.getMemberName(i);
            String type = MvSqlGen.formatType(st.getMemberType(i));
            if (i > 0) {
                sb.append(",");
            }
            safeId(sb, name).append(":").append(type);
        }
        sb.append(">");
        return sb;
    }

    public static StringBuilder structForTable(StringBuilder sb, MvTableInfo ti) {
        if (ti == null) {
            throw new NullPointerException();
        }
        if (sb == null) {
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
}
