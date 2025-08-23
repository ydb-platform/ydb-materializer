package tech.ydb.mv.apply;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import tech.ydb.mv.MvSqlGen;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 *
 * @author zinal
 */
public class MvActionGrabKeys implements MvApplyAction {

    private final String id;
    private final MvTarget target;
    private final String sourceTableName;
    private final MvActionContext context;
    private final String sqlSelect;

    public MvActionGrabKeys(MvTarget target, MvJoinSource js, MvActionContext context) {
        this.id = UUID.randomUUID().toString();
        this.target = target;
        this.context = context;
        this.sourceTableName = target.getName();
        try (MvSqlGen sg = new MvSqlGen(new MvKeyPathGenerator(target).generate(js))) {
            this.sqlSelect = sg.makeSelect();
        }
    }

    @Override
    public String toString() {
        return "GrabKeys{" + sourceTableName + " -> " + target.getName() + '}';
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
