package tech.ydb.mv.format;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvIssue;

/**
 *
 * @author zinal
 */
public class MvIssuePrinter {

    private final MvContext ctx;

    public MvIssuePrinter(MvContext ctx) {
        this.ctx = ctx;
    }

    public void write(PrintStream pw) {
        pw.println("Begin context status: " + (ctx.isValid() ? "VALID" : "INVALID"));
        pw.println("  Roots:    " + ctx.getTargets().size()
                + " target(s) and " + ctx.getHandlers().size() + " handler(s).");
        pw.println("  Inputs:   " + getInputsCount(false) + " streaming, " + getInputsCount(true) + " batch.");
        pw.println("  Messages: " + ctx.getErrors().size() + " errors, " + ctx.getWarnings().size() + " warnings.");
        for (MvIssue x : sort(ctx.getErrors())) {
            pw.print("\tERROR: ");
            pw.println(x.getMessage());
        }
        for (MvIssue x : sort(ctx.getWarnings())) {
            pw.print("\tWARNING: ");
            pw.println(x.getMessage());
        }
        pw.println("End context status");
    }

    public String write() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            write(ps);
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    private ArrayList<MvIssue> sort(Collection<MvIssue> issues) {
        ArrayList<MvIssue> output = new ArrayList<>(issues);
        output.sort((x, y) -> x.getSqlPos().compareTo(y.getSqlPos()));
        return output;
    }

    private int getInputsCount(boolean batchMode) {
        return (int) ctx.getHandlers().values().stream()
                .flatMap(h -> h.getInputs().values().stream())
                .filter(mi -> batchMode == mi.isBatchMode())
                .count();
    }
}
