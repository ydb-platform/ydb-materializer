package tech.ydb.mv.format;

import java.io.PrintStream;
import java.util.ArrayList;
import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.apply.MvKeyPathGenerator;

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
        pw.println("  ** Equivalent view:");
        pw.println();
        pw.println(sg.makeCreateView());
        pw.println("  ** Refresh statement:");
        pw.println();
        pw.println(sg.makeSelect());
        pw.println("  ** Upsert statement:");
        pw.println();
        pw.println(sg.makePlainUpsert());
        pw.println("  ** Delete statement:");
        pw.println();
        pw.println(sg.makePlainDelete());
        MvKeyPathGenerator pathGenerator = new MvKeyPathGenerator(mt);
        for (int pos = 1; pos < mt.getSources().size(); ++pos) {
            MvJoinSource js = mt.getSources().get(pos);
            MvTarget temp = pathGenerator.generate(js);
            MvSqlGen sgTemp = new MvSqlGen(temp);
            pw.println("  ** Key extraction, " + js.getTableName() + " as " + js.getTableAlias());
            pw.println();
            pw.println(sgTemp.makeSelect());
        }
    }

}
