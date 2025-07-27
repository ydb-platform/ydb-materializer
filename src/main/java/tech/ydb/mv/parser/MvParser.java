package tech.ydb.mv.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.ParserRuleContext;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvInputPosition;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvTableRef;
import tech.ydb.mv.model.MvTarget;

/**
 * Parsing, linking and logical checks for input SQL script.
 * @author mzinal
 */
public class MvParser {

    private final Lexer lexer;
    private final YdbMatViewV1Parser parser;
    private final YdbMatViewV1Parser.Sql_scriptContext root;

    public MvParser(CharStream cs) {
        this.lexer = new YdbMatViewV1Lexer(cs);
        this.parser = new YdbMatViewV1Parser(new CommonTokenStream(lexer));
        this.root = parser.sql_script();
    }

    public MvParser(InputStream is, Charset charset) throws IOException {
        this(CharStreams.fromStream(is, charset));
    }

    public MvParser(String input) {
        this(CharStreams.fromString(input));
    }

    public MvContext fill() {
        MvContext ctx = new MvContext();
        for ( var stmt : root.sql_stmt() ) {
            fill(ctx, stmt.create_mat_view_stmt());
            fill(ctx, stmt.process_stmt());
        }
        link(ctx);
        validate(ctx);
        return ctx;
    }

    private static MvInputPosition toInputPosition(ParserRuleContext ctx) {
        var p = ctx.getStart();
        if (p!=null) {
            return new MvInputPosition(p.getLine(), p.getCharPositionInLine());
        }
        return null;
    }

    private void fill(MvContext mc, YdbMatViewV1Parser.Create_mat_view_stmtContext stmt) {
        MvTarget mt = new MvTarget(toInputPosition(stmt));
        mc.getViews().add(mt);
        mt.setName(stmt.identifier().getText());
        var sel = stmt.simple_select_stmt();
        var src = new MvTableRef(toInputPosition(sel.main_table_ref()));
        mt.getSources().add(src);
        src.setReference(sel.main_table_ref().identifier().getText());
        src.setAlias(sel.table_alias().ID_PLAIN().getText());
        src.setMode(MvTableRef.Mode.MAIN);
        for (var part : sel.simple_join_part()) {
            fill(mt, part);
        }
        for (var cc : sel.result_column()) {
            fill(mt, cc);
        }
    }

    private void fill(MvTarget mt, YdbMatViewV1Parser.Simple_join_partContext part) {
        MvTableRef src = new MvTableRef(toInputPosition(part));
        mt.getSources().add(src);
        src.setReference(part.join_table_ref().identifier().getText());
        src.setAlias(part.table_alias().ID_PLAIN().getText());
        if (part.LEFT()!=null) {
            src.setMode(MvTableRef.Mode.LEFT);
        } else {
            src.setMode(MvTableRef.Mode.INNER);
        }
        for (var cond : part.join_condition()) {
            fill(src, cond);
        }
    }

    private void fill(MvTableRef src, YdbMatViewV1Parser.Join_conditionContext cond) {
        MvJoinCondition mjc = new MvJoinCondition(toInputPosition(cond));
        src.getConditions().add(mjc);
        if (cond.column_reference_first()!=null) {
            var v = cond.column_reference_first();
            mjc.setFirstAlias(v.table_alias().ID_PLAIN().getText());
            mjc.setFirstColumn(v.column_name().identifier().getText());
        }
        if (cond.column_reference_second()!=null) {
            var v = cond.column_reference_second();
            mjc.setSecondAlias(v.table_alias().ID_PLAIN().getText());
            mjc.setSecondColumn(v.column_name().identifier().getText());
        }
        if (cond.constant_first()!=null) {
            mjc.setFirstLiteral(cond.constant_first().getText());
        }
        if (cond.constant_second()!=null) {
            mjc.setSecondLiteral(cond.constant_second().getText());
        }
    }

    private void fill(MvTarget mt, YdbMatViewV1Parser.Result_columnContext cc) {
        var column = new MvColumn(toInputPosition(cc));
        mt.getColumns().add(column);
        column.setName(cc.column_alias().ID_PLAIN().getText());
        if (cc.opaque_expression()!=null) {
            MvComputation expr = new MvComputation(toInputPosition(cc.opaque_expression()));
            column.setComputation(expr);
            expr.setExpression(cc.opaque_expression()
                    .opaque_expression_body()
                    .opaque_expression_body_text()
                    .getText());
            for (var tabref : cc.opaque_expression().table_alias()) {
                expr.getSources().add(new MvComputation.Source(tabref.ID_PLAIN().getText()));
            }
        }
        if (cc.column_name()!=null) {
            column.setSourceColumn(cc.column_name().identifier().ID_PLAIN().getText());
        }
        if (cc.table_alias()!=null) {
            column.setSourceAlias(cc.table_alias().ID_PLAIN().getText());
        }
    }

    private void fill(MvContext mc, YdbMatViewV1Parser.Process_stmtContext stmt) {
        MvInput mi = new MvInput(toInputPosition(stmt));
        mc.getInputs().add(mi);
        mi.setTableName(stmt.main_table_ref().identifier().getText());
        mi.setChangeFeed(stmt.changefeed_name().identifier().getText());
        if (stmt.STREAM()!=null) {
            mi.setBatchMode(false);
        } else {
            mi.setBatchMode(true);
        }
    }

    public static void link(MvContext mc) {
        mc.getViews().stream().forEach(t -> link(t, mc));
    }

    private static void link(MvTarget t, MvContext mc) {
        t.getColumns().stream().forEach(c -> link(c, t, mc));
    }

    private static void link(MvColumn c, MvTarget t, MvContext mc) {
        if (c.isComputation()) {
            link(c.getComputation(), t, mc);
        } else {
            var ref = t.getSourceByName(c.getSourceAlias());
            c.setSourceRef(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, c.getSourceAlias(), c));
            }
        }
    }

    private static void link(MvComputation c, MvTarget t, MvContext mc) {
        for (var src : c.getSources()) {
            var ref = t.getSourceByName(src.getAlias());
            src.setReference(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, src.getAlias(), c));
            }
        }
    }

    public static void validate(MvContext mc) {

    }

}
