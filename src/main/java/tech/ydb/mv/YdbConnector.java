package tech.ydb.mv;

import java.lang.management.ManagementFactory;
import java.util.Properties;

import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.TableClient;
import tech.ydb.topic.TopicClient;

import tech.ydb.mv.svc.MvConnector;

/**
 * The helper class which creates the YDB connection from the set of properties.
 *
 * @author zinal
 */
public class YdbConnector implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbConnector.class);

    private final MvConfig config;
    private final GrpcTransport transStd;
    private final MvConnector.ConnStd connStd;
    private final GrpcTransport transMgt;
    private final MvConnector.ConnMgt connMgt;

    public YdbConnector(MvConfig config, boolean management) {
        this.config = config;

        LOG.info("Connecting to {}...", config.getConnectionString());

        GrpcTransport transStd = null;
        MvConnector.ConnStd connStd = null;
        GrpcTransport transMgt = null;
        MvConnector.ConnMgt connMgt = null;

        try {
            transStd = MvConnector.configure(config);
            connStd = new MvConnector.ConnStd(config, transStd);
            LOG.info("Main connection has been established.");
            if (management) {
                transMgt = MvConnector.configure(config);
                connMgt = new MvConnector.ConnMgt(config, transMgt);
                LOG.info("Management connection has been established.");
            }
        } catch (Exception ex) {
            if (connMgt != null) {
                connMgt.close();
            }
            if (connStd != null) {
                connStd.close();
            }
            if (transMgt != null) {
                transMgt.close();
            }
            if (transStd != null) {
                transStd.close();
            }
            throw ex;
        }
        this.transStd = transStd;
        this.connStd = connStd;
        this.transMgt = transMgt;
        this.connMgt = connMgt;
    }

    public YdbConnector(Properties props, boolean management) {
        this(new MvConfig(props), management);
    }

    public YdbConnector(String fname, boolean management) {
        this(MvConfig.fromFile(fname), management);
    }

    public YdbConnector(MvConfig config) {
        this(config, false);
    }

    public YdbConnector(String fname) {
        this(MvConfig.fromFile(fname), false);
    }

    public MvConnector.ConnStd getConnStd() {
        return connStd;
    }

    public MvConnector.ConnMgt getConnMgt() {
        if (connMgt == null) {
            throw new IllegalStateException("Managament connection has not been configured");
        }
        return connMgt;
    }

    public boolean isOpen() {
        return connStd.isOpen();
    }

    public String getDatabase() {
        return connStd.getDatabase();
    }

    public MvConfig getConfig() {
        return config;
    }

    public QueryClient getQueryClient() {
        return connStd.getQueryClient();
    }

    public SessionRetryContext getQueryRetryCtx() {
        return connStd.getQueryRetryCtx();
    }

    public TopicClient getTopicClient() {
        return connStd.getTopicClient();
    }

    public TableClient getTableClient() {
        return connStd.getTableClient();
    }

    public tech.ydb.table.SessionRetryContext getTableRetryCtx() {
        return connStd.getTableRetryCtx();
    }

    public String getProperty(String name) {
        return config.getProperty(name);
    }

    public String getProperty(String name, String defval) {
        return config.getProperty(name, defval);
    }

    public int getProperty(String name, int defval) {
        return config.getProperty(name, defval);
    }

    public long getProperty(String name, long defval) {
        return config.getProperty(name, defval);
    }

    public boolean getProperty(String name, boolean defval) {
        return config.getProperty(name, defval);
    }

    public String fullTableName(String tableName) {
        return connStd.fullTableName(tableName);
    }

    public String fullCdcTopicName(String tableName, String changefeed) {
        return connStd.fullCdcTopicName(tableName, changefeed);
    }

    public QuerySession createQuerySession() {
        return connStd.createQuerySession();
    }

    public QueryReader sqlReadWrite(String query, Params params) {
        return connStd.sqlReadWrite(query, params);
    }

    public QueryReader sqlRead(String query, Params params) {
        return connStd.sqlRead(query, params);
    }

    public void sqlWrite(String query, Params params) {
        connStd.sqlWrite(query, params);
    }

    @Override
    public void close() {
        dumpThreadsIfConfigured();
        LOG.info("Closing YDB connections...");
        if (connMgt != null) {
            connMgt.close();
        }
        if (connStd != null) {
            connStd.close();
        }
        if (transMgt != null) {
            transMgt.close();
        }
        if (transStd != null) {
            transStd.close();
        }
        LOG.info("Disconnected from YDB.");
    }

    private void dumpThreadsIfConfigured() {
        if (!config.getProperty("dump.threads.on.close", false)) {
            return;
        }
        LOG.info("Performing pre-closure thread dump.");
        var threadMXBean = ManagementFactory.getThreadMXBean();
        var threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (var threadInfo : threadInfos) {
            LOG.info("{} -> {}", threadInfo.getThreadName(), threadInfo.getThreadState());
            for (var ste : threadInfo.getStackTrace()) {
                LOG.info("\t {}", ste);
            }
            LOG.info("***");
        }
    }
}
