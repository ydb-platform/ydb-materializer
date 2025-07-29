package tech.ydb.mv.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvInputPosition;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * Parsing, linking and minimal logical checks for input SQL script.
 * @author mzinal
 */
public class MvParser {

    private final Lexer lexer;
    private final YdbMatViewV1Parser parser;
    private final YdbMatViewV1Parser.Sql_scriptContext root;
    private final ArrayList<MvIssue> issues = new ArrayList<>();

    public MvParser(CharStream cs) {
        this.lexer = new YdbMatViewV1Lexer(cs);
        this.lexer.addErrorListener(new LexerListener());
        this.parser = new YdbMatViewV1Parser(new CommonTokenStream(lexer));
        this.parser.setErrorHandler(new ParserListener());
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
        ctx.getErrors().addAll(issues);
        for ( var stmt : root.sql_stmt() ) {
            if (stmt.create_mat_view_stmt()!=null) {
                fill(ctx, stmt.create_mat_view_stmt());
            }
            if (stmt.process_stmt()!=null) {
                fill(ctx, stmt.process_stmt());
            }
        }
        link(ctx);
        return ctx;
    }

    private static MvInputPosition toInputPosition(ParserRuleContext ctx) {
        if (ctx==null) {
            return null;
        }
        var p = ctx.getStart();
        if (p!=null) {
            return new MvInputPosition(p.getLine(), p.getCharPositionInLine());
        }
        return null;
    }

    private void fill(MvContext mc, YdbMatViewV1Parser.Create_mat_view_stmtContext stmt) {
        MvTarget mt = new MvTarget(toInputPosition(stmt));
        mc.getTargets().add(mt);
        mt.setName(stmt.identifier().getText());
        var sel = stmt.simple_select_stmt();
        var src = new MvJoinSource(toInputPosition(sel.main_table_ref()));
        mt.getSources().add(src);
        src.setTableName(sel.main_table_ref().identifier().getText());
        src.setAlias(sel.table_alias().ID_PLAIN().getText());
        src.setMode(MvJoinSource.Mode.MAIN);
        for (var part : sel.simple_join_part()) {
            fill(mt, part);
        }
        for (var cc : sel.result_column()) {
            fill(mt, cc);
        }
        if (sel.opaque_expression()!=null) {
            fillCondition(mt, sel.opaque_expression());
        }
    }

    private void fillCondition(MvTarget mt, YdbMatViewV1Parser.Opaque_expressionContext cond) {
        MvComputation filter = new MvComputation(toInputPosition(cond));
        mt.setFilter(filter);
        filter.setExpression(getExpressionText(cond.opaque_expression_body()));
        for (var tabref : cond.table_alias()) {
            filter.getSources().add(new MvComputation.Source(tabref.ID_PLAIN().getText()));
        }
    }

    private static String getExpressionText(YdbMatViewV1Parser.Opaque_expression_bodyContext e) {
        String retval = e.OPAQUE_EXPRESSION().getText();
        if (retval.startsWith("#[")) {
            if (retval.endsWith("]#")) {
                // happy path
                retval = retval.substring(2, retval.length()-2);
            } else {
                retval = retval.substring(2, retval.length());
            }
        } else {
            if (retval.endsWith("]#")) {
                retval = retval.substring(0, retval.length()-2);
            }
        }
        return retval.trim();
    }

    private void fill(MvTarget mt, YdbMatViewV1Parser.Simple_join_partContext part) {
        MvJoinSource src = new MvJoinSource(toInputPosition(part));
        mt.getSources().add(src);
        src.setTableName(part.join_table_ref().identifier().getText());
        src.setAlias(part.table_alias().ID_PLAIN().getText());
        if (part.LEFT()!=null) {
            src.setMode(MvJoinSource.Mode.LEFT);
        } else {
            src.setMode(MvJoinSource.Mode.INNER);
        }
        for (var cond : part.join_condition()) {
            fill(mt, src, cond);
        }
    }

    private void fill(MvTarget mt, MvJoinSource src, YdbMatViewV1Parser.Join_conditionContext cond) {
        MvJoinCondition mjc = new MvJoinCondition(toInputPosition(cond));
        src.getConditions().add(mjc);
        if (cond.column_reference_first()!=null) {
            var v = cond.column_reference_first().column_reference();
            mjc.setFirstAlias(v.table_alias().ID_PLAIN().getText());
            mjc.setFirstColumn(v.column_name().identifier().getText());
        }
        if (cond.column_reference_second()!=null) {
            var v = cond.column_reference_second().column_reference();
            mjc.setSecondAlias(v.table_alias().ID_PLAIN().getText());
            mjc.setSecondColumn(v.column_name().identifier().getText());
        }
        if (cond.constant_first()!=null) {
            mjc.setFirstLiteral(mt.addLiteral(cond.constant_first().getText()));
        }
        if (cond.constant_second()!=null) {
            mjc.setSecondLiteral(mt.addLiteral(cond.constant_second().getText()));
        }
    }

    private void fill(MvTarget mt, YdbMatViewV1Parser.Result_columnContext cc) {
        var column = new MvColumn(toInputPosition(cc));
        mt.getColumns().add(column);
        column.setName(cc.column_alias().ID_PLAIN().getText());
        if (cc.opaque_expression()!=null) {
            MvComputation expr = new MvComputation(toInputPosition(cc.opaque_expression()));
            column.setComputation(expr);
            expr.setExpression(getExpressionText(cc.opaque_expression().opaque_expression_body()));
            for (var tabref : cc.opaque_expression().table_alias()) {
                expr.getSources().add(new MvComputation.Source(tabref.ID_PLAIN().getText()));
            }
        }
        if (cc.column_reference()!=null) {
            column.setSourceColumn(cc.column_reference().column_name().identifier().getText());
            column.setSourceAlias(cc.column_reference().table_alias().ID_PLAIN().getText());
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
        mc.getTargets().stream().forEach(t -> link(t, mc));
    }

    private static void link(MvTarget t, MvContext mc) {
        t.getColumns().stream().forEach(c -> link(c, t, mc));
        t.getSources().stream().forEach(s -> link(s, t, mc));
    }

    private static void link(MvColumn c, MvTarget t, MvContext mc) {
        if (c.isComputation()) {
            link(c.getComputation(), t, mc);
        } else {
            var ref = t.getSourceByAlias(c.getSourceAlias());
            c.setSourceRef(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, c.getSourceAlias(), c));
            }
        }
    }

    private static void link(MvComputation c, MvTarget t, MvContext mc) {
        for (var src : c.getSources()) {
            var ref = t.getSourceByAlias(src.getAlias());
            src.setReference(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, src.getAlias(), c));
            }
        }
    }

    private static void link(MvJoinSource s, MvTarget t, MvContext mc) {
        s.getConditions().stream().forEach(c -> link(c, t, mc));
    }

    private static void link(MvJoinCondition c, MvTarget t, MvContext mc) {
        if (c.getFirstAlias()!=null) {
            var ref = t.getSourceByAlias(c.getFirstAlias());
            c.setFirstRef(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, c.getFirstAlias(), c));
            }
        }
        if (c.getSecondAlias()!=null) {
            var ref = t.getSourceByAlias(c.getSecondAlias());
            c.setSecondRef(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, c.getSecondAlias(), c));
            }
        }
    }

    private class LexerListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                int line, int charPositionInLine, String msg, RecognitionException e) {
            issues.add(new MvIssue.LexerError(line, charPositionInLine, msg));
        }
    }

    private class ParserListener extends DefaultErrorStrategy {
        @Override
        public void recover(Parser recognizer, RecognitionException e) {
            String msg = e.getMessage();
            if (msg==null) {
                if (e.getOffendingToken() != null) {
                    msg = "Offending token: [" + e.getOffendingToken().getText() + "]";
                }
            }
            issues.add(new MvIssue.ParserError(
                    e.getOffendingToken().getLine(),
                    e.getOffendingToken().getCharPositionInLine(),
                    msg));
            super.recover(recognizer, e);
        }

        @Override
        public Token recoverInline(Parser recognizer)
                throws RecognitionException {
            issues.add(new MvIssue.ParserError(
                    recognizer.getCurrentToken().getLine(),
                    recognizer.getCurrentToken().getCharPositionInLine(),
                    "Unexpected token: " + recognizer.getCurrentToken().getText()));
            return super.recoverInline(recognizer);
        }
    }

}
