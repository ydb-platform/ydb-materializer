package tech.ydb.mv;

import java.util.HashMap;
import java.util.Properties;

import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

import tech.ydb.mv.impl.MvContextReader;
import tech.ydb.mv.impl.MvContextValidator;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvHandler;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;

/**
 * Work context for YDB Materializer activities.
 * @author zinal
 */
public class MvService implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvService.class);

    private final YdbConnector connector;
    private final MvContext context;

    public MvService(YdbConnector connector, Properties props) {
        this.connector = connector;
        this.context = MvContextReader.readContext(this.connector, props);
        refreshMetadata();
    }

    public MvService(YdbConnector.Config config) {
        this(new YdbConnector(config), config.getProperties());
    }

    public MvService(YdbConnector connector) {
        this(connector, connector.getConfig().getProperties());
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvContext getContext() {
        return context;
    }

    @Override
    public void close() {
        connector.close();
    }

    private void refreshMetadata() {
        if (! context.isValid()) {
            LOG.warn("Context is not valid after parsing - metadata retrieval skipped.");
            return;
        }
        HashMap<String, MvTableInfo> info = new HashMap<>();
        for (String tabname : context.collectTables()) {
            MvTableInfo ti = describeTable(tabname);
            if (ti!=null) {
                info.put(tabname, ti);
            }
        }
        linkTables(info);
        validate();
    }

    private void linkTables(HashMap<String, MvTableInfo> info) {
        for (MvTarget t : context.getTargets().values()) {
            for (MvJoinSource r : t.getSources()) {
                r.setTableInfo(info.get(r.getTableName()));
            }
        }
        for (MvHandler h : context.getHandlers().values()) {
            for (MvInput i : h.getInputs().values()) {
                i.setTableInfo(info.get(i.getTableName()));
            }
        }
    }

    private MvTableInfo describeTable(String tabname) {
        String path;
        if (tabname.startsWith("/")) {
            path = tabname;
        } else {
            path = connector.getDatabase() + "/" + tabname;
        }
        LOG.info("Describing table {} ...", path);
        TableDescription desc;
        try {
            DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            desc = connector.getTableRetryCtx()
                    .supplyResult(sess -> sess.describeTable(path, dts))
                    .join().getValue();
        } catch(Exception ex) {
            LOG.warn("Failed to obtain description for table {}", path, ex);
            return null;
        }

        return new MvTableInfo(tabname, path, desc);
    }

    private boolean validate() {
        if (! context.isValid()) {
            LOG.warn("Context already invalid, validation skipped.");
            return false;
        }
        return new MvContextValidator(context).validate();
    }
}
