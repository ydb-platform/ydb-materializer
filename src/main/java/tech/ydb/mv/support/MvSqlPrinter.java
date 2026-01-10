package tech.ydb.mv.support;

import java.io.PrintStream;
import java.util.ArrayList;

import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.parser.MvPathGenerator;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvViewPart;

/**
 *
 * @author zinal
 */
public class MvSqlPrinter {

    private final MvMetadata ctx;

    public MvSqlPrinter(MvMetadata ctx) {
        this.ctx = ctx;
    }

    public void write(PrintStream pw) {
        for (MvViewPart mt : sortTargets()) {
            write(pw, mt);
        }
    }

    private ArrayList<MvViewPart> sortTargets() {
        ArrayList<MvViewPart> output = new ArrayList<>();
        for (var mv : ctx.getViews().values()) {
            output.addAll(mv.getParts().values());
        }
        output.sort((x, y) -> compareTargets(x, y));
        return output;
    }

    private int compareTargets(MvViewPart x, MvViewPart y) {
        int cmp = x.getName().compareToIgnoreCase(y.getName());
        if (cmp == 0) {
            cmp = x.getAlias().compareToIgnoreCase(y.getAlias());
        }
        return cmp;
    }

    public void write(PrintStream pw, MvViewPart mt) {
        MvSqlGen sg = new MvSqlGen(mt);
        pw.println("-------------------------------------------------------");
        pw.println("*** Target: " + mt.getName() + " AS " + mt.getAlias());
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
        if (mt.getTableInfo() == null) {
            pw.println("  ** Skipped - no target table information.");
            pw.println();
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
        MvPathGenerator pathGenerator = new MvPathGenerator(mt);
        for (int pos = 1; pos < mt.getSources().size(); ++pos) {
            MvJoinSource js = mt.getSources().get(pos);
            if (!js.isTableKnown() || js.getInput() == null) {
                pw.println("  ** Skipped key extraction for incomplete "
                        + "join source " + js.getTableName() + " as " + js.getTableAlias());
                continue;
            }
            if (js.getInput().isBatchMode()) {
                // Key extraction not needed in batch mode
                continue;
            }
            MvViewPart temp = pathGenerator.extractKeysReverse(js);
            pw.println("  ** Key extraction, " + js.getTableName() + " as " + js.getTableAlias());
            pw.println();
            if (temp != null) {
                pw.println(new MvSqlGen(temp).makeSelect());
            } else {
                pw.println("<mapping is not possible>");
                pw.println();
            }
        }
    }

}
