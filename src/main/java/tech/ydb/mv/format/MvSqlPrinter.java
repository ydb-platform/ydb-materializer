package tech.ydb.mv.format;

import java.io.PrintStream;
import java.util.ArrayList;

import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.parser.MvKeyPathGenerator;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public class MvSqlPrinter {

    private final MvContext ctx;

    public MvSqlPrinter(MvContext ctx) {
        this.ctx = ctx;
    }

    public void write(PrintStream pw) {
        for (MvTarget mt : sortTargets()) {
            write(pw, mt);
        }
    }

    private ArrayList<MvTarget> sortTargets() {
        ArrayList<MvTarget> output = new ArrayList<>(ctx.getTargets().values());
        output.sort((x, y) -> x.getName().compareToIgnoreCase(y.getName()));
        return output;
    }

    public void write(PrintStream pw, MvTarget mt) {
        MvSqlGen sg = new MvSqlGen(mt);
        pw.println("-------------------------------------------------------");
        pw.println("*** Target: " + mt.getName());
        pw.println("-------------------------------------------------------");
        pw.println();
        pw.println("  ** Equivalent view DDL:");
        pw.println();
        pw.println(sg.makeCreateView());
        pw.println("  ** Destination table DDL:");
        pw.println();
        pw.println(sg.makeCreateTable());
        pw.println("  ** Refresh statement:");
        pw.println();
        pw.println(sg.makeSelect());
        pw.println("  ** Upsert statement:");
        pw.println();
        if (mt.getTableInfo()==null) {
            pw.println("  ** Skipped - no target table information.");
        } else {
            pw.println(sg.makePlainUpsert());
        }
        pw.println("  ** Delete statement:");
        pw.println();
        pw.println(sg.makePlainDelete());
        pw.println("  ** Topmost scan start:");
        pw.println();
        pw.println(sg.makeScanStart());
        pw.println("  ** Topmost scan next:");
        pw.println();
        pw.println(sg.makeScanNext());
        MvKeyPathGenerator pathGenerator = new MvKeyPathGenerator(mt);
        for (int pos = 1; pos < mt.getSources().size(); ++pos) {
            MvJoinSource js = mt.getSources().get(pos);
            if (!js.isTableKnown() || js.getInput()==null) {
                pw.println("  ** Skipped key extraction for incomplete "
                        + "join source " + js.getTableName() + " as " + js.getTableAlias());
                continue;
            }
            if (js.getInput().isBatchMode()) {
                // Key extraction not needed in batch mode
                continue;
            }
            MvTarget temp = pathGenerator.generate(js);
            pw.println("  ** Key extraction, " + js.getTableName() + " as " + js.getTableAlias());
            pw.println();
            if (temp!=null) {
                pw.println(new MvSqlGen(temp).makeSelect());
            } else {
                pw.println("<mapping is not possible>");
                pw.println();
            }
        }
    }

}
