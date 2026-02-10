package tech.ydb.mv.svc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.coordination.CoordinationClient;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import tech.ydb.mv.MvConfig;
import tech.ydb.query.QuerySession;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.topic.TopicClient;

/**
 * Shared code for different specialized YDB connectors
 *
 * @author zinal
 */
public class MvConnector implements AutoCloseable {

    protected static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvConnector.class);

    protected final MvConfig config;
    protected final GrpcTransport transport;
    protected final String database;
    protected final QueryClient queryClient;
    protected final SessionRetryContext queryRetryCtx;
    private final AtomicBoolean opened = new AtomicBoolean(false);

    protected MvConnector(MvConfig config, GrpcTransport transport, int maxSessions) {
        this.config = config;
        this.transport = transport;
        this.database = transport.getDatabase();
        this.queryClient = QueryClient.newClient(this.transport)
                .sessionPoolMinSize(1)
                .sessionPoolMaxSize(maxSessions)
                .build();
        this.queryRetryCtx = tech.ydb.query.tools.SessionRetryContext
                .create(this.queryClient)
                .sessionCreationTimeout(Duration.ofSeconds(10L))
                .idempotent(true)
                .build();
    }

    protected final void setOpened() {
        this.opened.set(true);
    }

    protected final void unsetOpened() {
        this.opened.set(false);
    }

    public boolean isOpen() {
        return opened.get();
    }

    public String getDatabase() {
        return database;
    }

    public MvConfig getConfig() {
        return config;
    }

    public GrpcTransport getTransport() {
        return transport;
    }

    public QueryClient getQueryClient() {
        return queryClient;
    }

    public SessionRetryContext getQueryRetryCtx() {
        return queryRetryCtx;
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
        if (tableName == null) {
            return null;
        }
        while (tableName.endsWith("/")) {
            tableName = tableName.substring(0, tableName.length() - 1);
        }
        if (tableName.startsWith("/")) {
            return tableName;
        }
        return database + "/" + tableName;
    }

    public String fullCdcTopicName(String tableName, String changefeed) {
        return fullTableName(tableName) + "/" + changefeed;
    }

    public QuerySession createQuerySession() {
        return queryClient.createSession(Duration.ofSeconds(60))
                .join().getValue();
    }

    public QueryReader sqlReadWrite(String query, Params params) {
        return queryRetryCtx.supplyResult(
                qs -> QueryReader.readFrom(
                        qs.createQuery(query, TxMode.SERIALIZABLE_RW, params)
                )).join().getValue();
    }

    public QueryReader sqlRead(String query, Params params) {
        return queryRetryCtx.supplyResult(
                qs -> QueryReader.readFrom(
                        qs.createQuery(query, TxMode.SNAPSHOT_RO, params)
                )).join().getValue();
    }

    public void sqlWrite(String query, Params params) {
        queryRetryCtx.supplyResult(
                qs -> qs.createQuery(query, TxMode.SERIALIZABLE_RW, params).execute()
        ).join().getStatus().expectSuccess();
    }

    @Override
    public void close() {
        unsetOpened();
        if (queryClient != null) {
            try {
                queryClient.close();
            } catch (Exception ex) {
                LOG.warn("QueryClient closing threw an exception", ex);
            }
        }
        // transport should not be closed here - it is managed on the upper context
    }

    public static GrpcTransport configure(MvConfig config) {
        GrpcTransportBuilder builder = GrpcTransport
                .forConnectionString(config.getConnectionString());
        switch (config.getAuthMode()) {
            case ENV:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getAuthProviderFromEnviron());
                break;
            case STATIC:
                if (config.getStaticLogin() == null || config.getStaticPassword() == null) {
                    throw new IllegalArgumentException("Login or password is missing for STATIC authentication");
                }
                builder = builder.withAuthProvider(
                        new StaticCredentials(config.getStaticLogin(), config.getStaticPassword()));
                break;
            case METADATA:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getMetadataAuthProvider());
                break;
            case SAKEY:
                if (config.getSaKeyFile() == null) {
                    throw new IllegalArgumentException("Service account file is missing for SAKEY authentication");
                }
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getServiceAccountFileAuthProvider(config.getSaKeyFile()));
                break;
            case NONE:
                break;
        }
        String tlsCertFile = config.getTlsCertificateFile();
        if (tlsCertFile != null && tlsCertFile.length() > 0) {
            byte[] cert;
            try {
                cert = Files.readAllBytes(Paths.get(tlsCertFile));
            } catch (IOException ix) {
                throw new RuntimeException("Failed to read file " + tlsCertFile, ix);
            }
            builder.withSecureConnection(cert);
        }
        if (config.isPreferLocalDc()) {
            builder = builder.withBalancingSettings(BalancingSettings.detectLocalDs());
        }

        builder = builder.withApplicationName("ydb-materializer");
        builder = builder.withClientProcessId(String.valueOf(ProcessHandle.current().pid()));

        return builder.build();
    }

    /**
     * "Standard" YDB connection, with its specific set of components.
     */
    public static class ConnStd extends MvConnector {

        private final TopicClient topicClient;
        private final TableClient tableClient;
        private final tech.ydb.table.SessionRetryContext tableRetryCtx;

        public ConnStd(MvConfig config, GrpcTransport transport) {
            super(config, transport, config.getPoolSize());
            this.topicClient = TopicClient.newClient(this.transport)
                    .setCompressionExecutor(Runnable::run) // Prevent OOM
                    .build();
            this.tableClient = QueryClient.newTableClient(this.transport)
                    .sessionPoolSize(1, config.getPoolSize())
                    .build();
            this.tableRetryCtx = tech.ydb.table.SessionRetryContext
                    .create(this.tableClient)
                    .sessionCreationTimeout(Duration.ofSeconds(5L))
                    .idempotent(true)
                    .build();
            setOpened();
        }

        public TopicClient getTopicClient() {
            return topicClient;
        }

        public TableClient getTableClient() {
            return tableClient;
        }

        public tech.ydb.table.SessionRetryContext getTableRetryCtx() {
            return tableRetryCtx;
        }

        @Override
        public void close() {
            unsetOpened();
            if (topicClient != null) {
                try {
                    topicClient.close();
                } catch (Exception ex) {
                    LOG.warn("TopicClient closing threw an exception", ex);
                }
            }
            if (tableClient != null) {
                try {
                    tableClient.close();
                } catch (Exception ex) {
                    LOG.warn("TableClient closing threw an exception", ex);
                }
            }
            super.close();
        }
    }

    /**
     * "Management" type connector.
     */
    public static class ConnMgt extends MvConnector {

        private final CoordinationClient coordinationClient;

        public ConnMgt(MvConfig config, GrpcTransport transport) {
            super(config, transport, 5);
            this.coordinationClient = CoordinationClient.newClient(this.transport);
            setOpened();
        }

        public CoordinationClient getCoordinationClient() {
            return coordinationClient;
        }

        @Override
        public void close() {
            unsetOpened();
            // coordinationClient does not support specific closing
            super.close();
        }
    }
}
