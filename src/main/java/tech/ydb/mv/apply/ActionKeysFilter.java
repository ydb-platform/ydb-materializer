package tech.ydb.mv.apply;

import java.util.List;
import tech.ydb.table.values.StructType;

/**
 *
 * @author zinal
 */
class ActionKeysFilter extends ActionBase implements MvApplyAction {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ActionKeysFilter.class);

    public ActionKeysFilter(MvActionContext context) {
        super(context);
    }

    @Override
    protected StructType getRowType() {
        return super.getRowType(); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    @Override
    protected String getSqlSelect() {
        return super.getSqlSelect(); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/OverriddenMethodBody
    }

    @Override
    public void apply(List<MvApplyTask> input) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
