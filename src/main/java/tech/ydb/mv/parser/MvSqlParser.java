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
import org.antlr.v4.runtime.tree.ParseTree;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.model.MvColumn;
import tech.ydb.mv.model.MvComputation;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvSqlPos;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvJoinCondition;
import tech.ydb.mv.model.MvJoinMode;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * Parsing, linking and minimal logical checks for input SQL script.
 * @author zinal
 */
public class MvSqlParser {

    private final Lexer lexer;
    private final YdbMatViewV1Parser parser;
    private final YdbMatViewV1Parser.Sql_scriptContext root;
    private final ArrayList<MvIssue> parseTimeIssues = new ArrayList<>();

    public MvSqlParser(CharStream cs) {
        this.lexer = new YdbMatViewV1Lexer(cs);
        this.lexer.addErrorListener(new LexerListener());
        this.parser = new YdbMatViewV1Parser(new CommonTokenStream(lexer));
        this.parser.setErrorHandler(new ParserListener());
        this.root = parser.sql_script();
    }

    public MvSqlParser(InputStream is, Charset charset) throws IOException {
        this(CharStreams.fromStream(is, charset));
    }

    public MvSqlParser(String input) {
        this(CharStreams.fromString(input));
    }

    public MvContext fill() {
        MvContext ctx = new MvContext();
        ctx.addIssues(parseTimeIssues);
        // first pass: add MV definitions as MvTarget objects
        for ( var stmt : root.sql_stmt() ) {
            if (stmt.create_mat_view_stmt()!=null) {
                fillTarget(ctx, stmt.create_mat_view_stmt());
            }
        }
        // second pass: add handler definitions
        for ( var stmt : root.sql_stmt() ) {
            if (stmt.create_handler_stmt()!=null) {
                fillHandler(ctx, stmt.create_handler_stmt());
            }
        }
        link(ctx);
        return ctx;
    }

    private static MvSqlPos toSqlPos(ParserRuleContext ctx) {
        if (ctx==null) {
            return null;
        }
        var p = ctx.getStart();
        if (p!=null) {
            return new MvSqlPos(p.getLine(), p.getCharPositionInLine());
        }
        return null;
    }

    private void fillTarget(MvContext mc, YdbMatViewV1Parser.Create_mat_view_stmtContext stmt) {
        var mt = new MvTarget(unquote(stmt.identifier()), toSqlPos(stmt));
        var sel = stmt.simple_select_stmt();
        var src = new MvJoinSource(toSqlPos(sel.main_table_ref()));
        mt.getSources().add(src);
        src.setTableName(unquote(sel.main_table_ref().identifier()));
        src.setTableAlias(unquote(sel.table_alias().ID_PLAIN()));
        src.setMode(MvJoinMode.MAIN);
        for (var part : sel.simple_join_part()) {
            fillJoinSource(mt, part);
        }
        for (var cc : sel.result_column()) {
            fillColumn(mt, cc);
        }
        if (sel.opaque_expression()!=null) {
            fillCondition(mt, sel.opaque_expression());
        }
        var prev = mc.addTarget(mt);
        if (prev!=null) {
            mc.addIssue(new MvIssue.DuplicateTarget(mt, prev));
        }
    }

    private void fillCondition(MvTarget mt, YdbMatViewV1Parser.Opaque_expressionContext cond) {
        MvComputation filter = new MvComputation(
                getExpressionText(cond.opaque_expression_body()),
                toSqlPos(cond)
        );
        mt.setFilter(filter);
        for (var tabref : cond.table_alias()) {
            filter.getSources().add(new MvComputation.Source(unquote(tabref.ID_PLAIN())));
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

    private void fillJoinSource(MvTarget mt, YdbMatViewV1Parser.Simple_join_partContext part) {
        MvJoinSource src = new MvJoinSource(toSqlPos(part));
        mt.getSources().add(src);
        if (part.join_table_ref()!=null) {
            src.setTableName(unquote(part.join_table_ref().identifier()));
        }
        if (part.table_alias()!=null) {
            src.setTableAlias(unquote(part.table_alias().ID_PLAIN()));
        }
        if (part.LEFT()!=null) {
            src.setMode(MvJoinMode.LEFT);
        } else {
            src.setMode(MvJoinMode.INNER);
        }
        for (var cond : part.join_condition()) {
            fillJoinCondition(mt, src, cond);
        }
    }

    private void fillJoinCondition(MvTarget mt, MvJoinSource src, YdbMatViewV1Parser.Join_conditionContext cond) {
        MvJoinCondition mjc = new MvJoinCondition(toSqlPos(cond));
        src.getConditions().add(mjc);
        if (cond.column_reference_first()!=null) {
            var v = cond.column_reference_first().column_reference();
            mjc.setFirstAlias(unquote(v.table_alias().ID_PLAIN()));
            mjc.setFirstColumn(unquote(v.column_name().identifier()));
        }
        if (cond.column_reference_second()!=null) {
            var v = cond.column_reference_second().column_reference();
            mjc.setSecondAlias(unquote(v.table_alias().ID_PLAIN()));
            mjc.setSecondColumn(unquote(v.column_name().identifier()));
        }
        if (cond.constant_first()!=null) {
            mjc.setFirstLiteral(mt.addLiteral(cond.constant_first().getText()));
        }
        if (cond.constant_second()!=null) {
            mjc.setSecondLiteral(mt.addLiteral(cond.constant_second().getText()));
        }
    }

    private void fillColumn(MvTarget mt, YdbMatViewV1Parser.Result_columnContext cc) {
        var column = new MvColumn(
                unquote(cc.column_alias().ID_PLAIN()),
                toSqlPos(cc));
        mt.getColumns().add(column);
        if (cc.opaque_expression()!=null) {
            MvComputation expr = new MvComputation(
                    getExpressionText(cc.opaque_expression().opaque_expression_body()),
                    toSqlPos(cc.opaque_expression())
            );
            column.setComputation(expr);
            for (var tabref : cc.opaque_expression().table_alias()) {
                expr.getSources().add(new MvComputation.Source(unquote(tabref.ID_PLAIN())));
            }
        }
        if (cc.column_reference()!=null) {
            column.setSourceColumn(unquote(cc.column_reference().column_name().identifier()));
            column.setSourceAlias(unquote(cc.column_reference().table_alias().ID_PLAIN()));
        }
    }

    private void fillHandler(MvContext mc, YdbMatViewV1Parser.Create_handler_stmtContext stmt) {
        var mh = new MvHandler(unquote(stmt.identifier()), toSqlPos(stmt));
        if (stmt.consumer_name()!=null) {
            mh.setConsumerName(unquote(stmt.consumer_name().identifier()));
        }
        for (var part : stmt.handler_part()) {
            if (part.handler_input_part()!=null) {
                fillHandlerInput(mc, mh, part.handler_input_part());
            }
            if (part.handler_process_part()!=null) {
                fillHandlerProcess(mc, mh, part.handler_process_part());
            }
        }
        var prev = mc.addHandler(mh);
        if (prev!=null) {
            mc.addIssue(new MvIssue.DuplicateHandler(mh, prev));
        }
        if (MvConfig.DICTINARY_HANDLER.equalsIgnoreCase(mh.getName())) {
            mc.addIssue(new MvIssue.IllegalHandlerName(mh));
        }
    }

    private void fillHandlerInput(MvContext mc, MvHandler mh, YdbMatViewV1Parser.Handler_input_partContext part) {
        MvInput mi = new MvInput(
                unquote(part.main_table_ref().identifier()),
                unquote(part.changefeed_name().identifier()),
                toSqlPos(part));
        if (part.STREAM()!=null) {
            mi.setBatchMode(false);
        } else {
            mi.setBatchMode(true);
        }
        MvInput prev = mh.addInput(mi);
        if (prev!=null) {
            mc.addIssue(new MvIssue.DuplicateInput(mi, prev));
        }
    }

    private void fillHandlerProcess(MvContext mc, MvHandler mh, YdbMatViewV1Parser.Handler_process_partContext part) {
        String mvName = unquote(part.mat_view_ref().identifier());
        MvTarget target = mc.getTargets().get(mvName);
        if (target==null) {
            mc.addIssue(new MvIssue.UnknownTarget(mh, mvName));
        } else {
            mh.addTarget(target);
        }
    }

    private static String unquote(ParseTree node) {
        String v = node.getText();
        if (v.length() > 2 && v.startsWith("`") && v.endsWith("`")) {
            v = v.substring(1, v.length()-1);
        }
        return v;
    }

    public static void link(MvContext mc) {
        mc.getTargets().values().forEach(t -> linkTarget(t, mc));
        mc.getHandlers().values().forEach(h -> linkHandler(h, mc));
    }

    private static void linkTarget(MvTarget t, MvContext mc) {
        t.getColumns().stream().forEach(c -> linkColumn(c, t, mc));
        t.getSources().stream().forEach(s -> linkSource(s, t, mc));
    }

    private static void linkHandler(MvHandler mh, MvContext mc) {
        for (MvTarget mt : mh.getTargets().values()) {
            for (MvJoinSource mjs : mt.getSources()) {
                linkHandlerJoinSource(mt, mjs, mh, mc);
            }
        }
    }

    private static void linkColumn(MvColumn c, MvTarget t, MvContext mc) {
        if (c.isComputation()) {
            linkComputation(c.getComputation(), t, mc);
        } else {
            var ref = t.getSourceByAlias(c.getSourceAlias());
            c.setSourceRef(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, c.getSourceAlias(), c));
            }
        }
    }

    private static void linkComputation(MvComputation c, MvTarget t, MvContext mc) {
        for (var src : c.getSources()) {
            var ref = t.getSourceByAlias(src.getAlias());
            src.setReference(ref);
            if (ref==null) {
                mc.addIssue(new MvIssue.UnknownAlias(t, src.getAlias(), c));
            }
        }
    }

    private static void linkSource(MvJoinSource s, MvTarget t, MvContext mc) {
        s.getConditions().stream().forEach(c -> linkJoinCondition(c, t, mc));
    }

    private static void linkJoinCondition(MvJoinCondition c, MvTarget t, MvContext mc) {
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

    private static void linkHandlerJoinSource(MvTarget target, MvJoinSource mjs, MvHandler mh, MvContext mc) {
        MvInput input = mh.getInput(mjs.getTableName());
        if (input != null) {
            mjs.setInput(input);
            input.addReference(target, mjs);
        }
    }

    private class LexerListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                int line, int charPositionInLine, String msg, RecognitionException e) {
            parseTimeIssues.add(new MvIssue.LexerError(line, charPositionInLine, msg));
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
            parseTimeIssues.add(new MvIssue.ParserError(
                    e.getOffendingToken().getLine(),
                    e.getOffendingToken().getCharPositionInLine(),
                    msg));
            super.recover(recognizer, e);
        }

        @Override
        public Token recoverInline(Parser recognizer)
                throws RecognitionException {
            parseTimeIssues.add(new MvIssue.ParserError(
                    recognizer.getCurrentToken().getLine(),
                    recognizer.getCurrentToken().getCharPositionInLine(),
                    "Unexpected token: " + recognizer.getCurrentToken().getText()));
            return super.recoverInline(recognizer);
        }
    }

}
