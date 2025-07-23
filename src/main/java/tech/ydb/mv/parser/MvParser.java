package tech.ydb.mv.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvInput;
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
        for ( YdbMatViewV1Parser.Sql_stmtContext stmt : root.sql_stmt() ) {
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
        YdbMatViewV1Parser.Simple_select_stmtContext sel = stmt.simple_select_stmt();
        mt.getSources().add(new MvTableRef(
                sel.main_table_ref().identifier().getText(),
                sel.table_alias().ID_PLAIN().getText(),
                MvTableRef.Mode.MAIN
        ));
        for (YdbMatViewV1Parser.Simple_join_partContext part : sel.simple_join_part()) {
            handle(mt, part);
        }
        mc.getViews().add(mt);
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
        for (YdbMatViewV1Parser.Join_conditionContext cond : part.join_condition()) {
            String rightAlias = cond.column_reference_right().table_alias().ID_PLAIN().getText();
            if (! rightAlias.equalsIgnoreCase(src.getAlias())) {
                throw new RuntimeException("Illegal syntax of join condition for table `"
                        + src.getReference() + "`: expected `" + src.getAlias() + "` on the right side, "
                        + "got `" + rightAlias + "` instead.");
            }
            MvJoinCondition jc = new MvJoinCondition(src, cond.column_reference_right().column_name().identifier().getText());
            if (cond.column_reference_left()!=null) {

            } else {

            }
        }
        mt.getSources().add(src);
    }

    private void handle(MvContext mc, YdbMatViewV1Parser.Process_stmtContext stmt) {
        MvInput mi = new MvInput(stmt.main_table_ref().identifier().getText(),
                stmt.changefeed_name().identifier().getText());
        mc.getInputs().add(mi);
    }

}
