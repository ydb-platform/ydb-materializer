package tech.ydb.mv.apply;

import java.util.List;
import java.util.Objects;

import tech.ydb.table.values.StructType;

import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public class MvActionGrabKeys extends MvActionBase implements MvApplyAction {

    private final MvApplyManager applyManager;
    private final MvTarget target;
    private final MvTarget transformation;
    private final String inputTableName;
    private final String inputTableAlias;
    private final String sqlSelect;
    private final StructType rowType;

    public MvActionGrabKeys(MvApplyManager applyManager, MvTarget target,
            MvJoinSource js, MvActionContext context) {
        super(context);
        this.applyManager = applyManager;
        this.target = target;
        this.transformation = new MvKeyPathGenerator(target).generate(js);
        this.inputTableName = js.getTableName();
        this.inputTableAlias = js.getTableAlias();
        try (MvSqlGen sg = new MvSqlGen(this.transformation)) {
            this.sqlSelect = sg.makeSelect();
            this.rowType = sg.toRowType();
        }
    }

    @Override
    public String getSqlSelect() {
        return sqlSelect;
    }

    @Override
    public StructType getRowType() {
        return rowType;
    }

    @Override
    public String toString() {
        return "MvActionGrabKeys{" + inputTableName
                + " AS " + inputTableAlias + " -> "
                + target.getName() + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + Objects.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvActionGrabKeys other = (MvActionGrabKeys) obj;
        return Objects.equals(this.id, other.id);
    }

    @Override
    public void apply(List<MvApplyTask> input) {
        if (input==null || input.isEmpty()) {
            return;
        }
    }

}
