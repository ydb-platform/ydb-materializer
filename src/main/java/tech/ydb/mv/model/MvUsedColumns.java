package tech.ydb.mv.model;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Collect and present column usage information.
 *
 * @author zinal
 */
public class MvUsedColumns {

    // alias -> usage
    private final HashMap<String, JoinSource> tables = new HashMap<>();

    public void fill(MvViewPart target) {
        for (MvColumn c : target.getColumns()) {
            if (c.isComputation()) {
                for (var src : c.getComputation().getSources()) {
                    makeSource(src.getReference()).addColumn(src.getColumn());
                }
            }
            if (c.isReference()) {
                makeSource(c.getSourceRef()).addColumn(c.getSourceColumn());
            }
        }
        for (MvJoinSource js : target.getSources()) {
            for (MvJoinCondition cond : js.getConditions()) {
                if (cond.getFirstRef() != null) {
                    makeSource(cond.getFirstRef()).addColumn(cond.getFirstColumn());
                }
                if (cond.getSecondRef() != null) {
                    makeSource(cond.getSecondRef()).addColumn(cond.getSecondColumn());
                }
            }
        }
        if (target.getFilter() != null) {
            for (var src : target.getFilter().getSources()) {
                makeSource(src.getReference()).addColumn(src.getColumn());
            }
        }
    }

    public boolean isColumnUsed(String alias, String column) {
        JoinSource x = tables.get(alias);
        if (x == null) {
            return false;
        }
        return x.contains(column);
    }

    private JoinSource makeSource(MvJoinSource reference) {
        JoinSource js = tables.get(reference.getTableAlias());
        if (js==null) {
            js = new JoinSource(reference);
            tables.put(reference.getTableAlias(), js);
        }
        return js;
    }

    public static class JoinSource {
        private final MvJoinSource reference;
        private final HashSet<String> columns;

        public JoinSource(MvJoinSource reference) {
            this.reference = reference;
            this.columns = new HashSet<>();
        }

        public MvJoinSource getReference() {
            return reference;
        }

        public JoinSource addColumn(String name) {
            columns.add(name);
            return this;
        }

        public boolean contains(String name) {
            return columns.contains(name);
        }
    }

}
