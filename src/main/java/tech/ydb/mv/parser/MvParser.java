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
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvTableRef;
import tech.ydb.mv.model.MvTarget;

/**
 *
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
            handle(ctx, stmt.create_mat_view_stmt());
            handle(ctx, stmt.process_stmt());
        }
        link(ctx);
        return ctx;
    }

    private void link(MvContext mc) {

    }

    private void handle(MvContext mc, YdbMatViewV1Parser.Create_mat_view_stmtContext stmt) {
        MvTarget mt = new MvTarget(stmt.identifier().getText());
        mt.setInputPosition(toInputPosition(stmt));
        mc.getViews().add(mt);
        var sel = stmt.simple_select_stmt();
        var src = new MvTableRef(
                sel.main_table_ref().identifier().getText(),
                sel.table_alias().ID_PLAIN().getText(),
                MvTableRef.Mode.MAIN
        );
        src.setInputPosition(toInputPosition(sel.main_table_ref()));
        mt.getSources().add(src);
        for (var part : sel.simple_join_part()) {
            handle(mt, part);
        }
        for (var cc : sel.result_column()) {
            handle(mt, cc);
        }
    }

    private void handle(MvTarget mt, YdbMatViewV1Parser.Simple_join_partContext part) {
        MvTableRef.Mode mode = MvTableRef.Mode.INNER;
        if (part.LEFT()!=null) {
            mode = MvTableRef.Mode.LEFT;
        }
        MvTableRef src = new MvTableRef (
            part.join_table_ref().identifier().getText(),
            part.table_alias().ID_PLAIN().getText(),
            mode
        );
        src.setInputPosition(toInputPosition(part));
        mt.getSources().add(src);
        for (var cond : part.join_condition()) {
            handle(src, cond);
        }
    }

    private void handle(MvTableRef src, YdbMatViewV1Parser.Join_conditionContext cond) {
        MvJoinCondition mjc = new MvJoinCondition();
        mjc.setInputPosition(toInputPosition(cond));
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

    private void handle(MvTarget mt, YdbMatViewV1Parser.Result_columnContext cc) {
        var column = new MvColumn();
        column.setInputPosition(toInputPosition(cc));
        mt.getColumns().add(column);
        column.setName(cc.column_alias().ID_PLAIN().getText());
        if (cc.opaque_expression()!=null) {
            MvComputation expr = new MvComputation(cc.opaque_expression()
                    .opaque_expression_body()
                    .opaque_expression_body_text()
                    .getText());
            expr.setInputPosition(toInputPosition(cc.opaque_expression()));
            column.setComputation(expr);
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

    private void handle(MvContext mc, YdbMatViewV1Parser.Process_stmtContext stmt) {
        MvInput mi = new MvInput(stmt.main_table_ref().identifier().getText(),
                stmt.changefeed_name().identifier().getText());
        mi.setInputPosition(toInputPosition(stmt));
        mc.getInputs().add(mi);
    }

    private MvInputPosition toInputPosition(ParserRuleContext ctx) {
        var p = ctx.getStart();
        if (p!=null) {
            return new MvInputPosition(p.getLine(), p.getCharPositionInLine());
        }
        return null;
    }

}
