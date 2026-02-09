package tech.ydb.mv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.management.ManagementFactory;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.BalancingSettings;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.coordination.CoordinationClient;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.topic.TopicClient;

/**
 * The helper class which creates the YDB connection from the set of properties.
 *
 * @author zinal
 */
public class YdbConnector implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbConnector.class);

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final QueryClient queryClient;
    private final tech.ydb.table.SessionRetryContext tableRetryCtx;
    private final tech.ydb.query.tools.SessionRetryContext queryRetryCtx;
    private final TopicClient topicClient;
    private final CoordinationClient coordinationClient;
    private final String database;
    private final Config config;
    private final AtomicBoolean opened = new AtomicBoolean(false);

    public YdbConnector(Config config) {
        LOG.info("Connecting to {}...", config.getConnectionString());
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

        GrpcTransport tempTransport = builder.build();
        this.database = tempTransport.getDatabase();
        try {
            this.coordinationClient = CoordinationClient.newClient(tempTransport);
            this.topicClient = TopicClient.newClient(tempTransport)
                    .setCompressionExecutor(Runnable::run) // Prevent OOM
                    .build();
            this.tableClient = QueryClient.newTableClient(tempTransport)
                    .sessionPoolSize(1, config.getPoolSize())
                    .build();
            this.tableRetryCtx = tech.ydb.table.SessionRetryContext
                    .create(this.tableClient)
                    .sessionCreationTimeout(Duration.ofSeconds(5L))
                    .idempotent(true)
                    .build();
            this.queryClient = QueryClient.newClient(tempTransport)
                    .sessionPoolMinSize(1)
                    .sessionPoolMaxSize(config.getPoolSize())
                    .build();
            this.queryRetryCtx = tech.ydb.query.tools.SessionRetryContext
                    .create(this.queryClient)
                    .sessionCreationTimeout(Duration.ofSeconds(5L))
                    .idempotent(true)
                    .build();
            this.transport = tempTransport;
            tempTransport = null; // to avoid closing below
        } finally {
            if (tempTransport != null) {
                tempTransport.close();
            }
        }
        this.config = config;
        this.opened.set(true);
    }

    public YdbConnector(Properties props) {
        this(new Config(props));
    }

    public YdbConnector(Properties props, String prefix) {
        this(new Config(props, prefix));
    }

    public YdbConnector(String fname, String prefix) {
        this(Config.fromFile(fname, prefix));
    }

    public YdbConnector(String fname) {
        this(Config.fromFile(fname));
    }

    public Config getConfig() {
        return config;
    }

    public CoordinationClient getCoordinationClient() {
        return coordinationClient;
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

    public tech.ydb.query.tools.SessionRetryContext getQueryRetryCtx() {
        return queryRetryCtx;
    }

    public QueryClient getQueryClient() {
        return queryClient;
    }

    public String getDatabase() {
        return database;
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
        opened.set(false);
        dumpThreadsIfConfigured();
        LOG.info("Closing YDB connections...");
        // coordinationClient does not support closing, so we leave it as is
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
        if (queryClient != null) {
            try {
                queryClient.close();
            } catch (Exception ex) {
                LOG.warn("QueryClient closing threw an exception", ex);
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception ex) {
                LOG.warn("GrpcTransport closing threw an exception", ex);
            }
        }
        LOG.info("Disconnected from YDB.");
    }

    private void dumpThreadsIfConfigured() {
        if (!getProperty("dump.threads.on.close", false)) {
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

    public boolean isOpen() {
        return opened.get();
    }

    public String getProperty(String name) {
        return config.properties.getProperty(name);
    }

    public String getProperty(String name, String defval) {
        return config.properties.getProperty(name, defval);
    }

    public int getProperty(String name, int defval) {
        String v = config.properties.getProperty(name, String.valueOf(defval));
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("[" + name + "]" + " Cannot parse as integer: " + v, nfe);
        }
    }

    public long getProperty(String name, long defval) {
        String v = config.properties.getProperty(name, String.valueOf(defval));
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("[" + name + "]" + " Cannot parse as long: " + v, nfe);
        }
    }

    public boolean getProperty(String name, boolean defval) {
        String v = config.properties.getProperty(name, String.valueOf(defval));
        return Boolean.parseBoolean(v);
    }

    public static String safe(String value) {
        return value.replaceAll("[;.$`'\\\"()\\\\]", "_");
    }

    /**
     * Configuration class for YDB database connections. It holds various
     * properties for connection strings, authentication settings, TLS
     * certificate files, connection pool size, and a prefix used for property
     * lookups. The configuration can be initialized with or without a set of
     * Java Properties, optionally using a custom prefix for property names. A
     * static method provided by the class allows loading configuration from an
     * external XML properties file.
     */
    public static final class Config {

        private String connectionString;
        private AuthMode authMode = AuthMode.NONE;
        private String saKeyFile;
        private String staticLogin;
        private String staticPassword;
        private String tlsCertificateFile;
        private int poolSize = 2 * (1 + Runtime.getRuntime().availableProcessors());
        private boolean preferLocalDc = false;
        private final String prefix;
        private final Properties properties = new Properties();

        public Config() {
            this.prefix = "ydb.";
            this.connectionString = "/local";
        }

        public Config(Properties props) {
            this(props, null);
        }

        public Config(Properties props, String prefix) {
            if (prefix == null) {
                prefix = "ydb.";
            }
            this.prefix = prefix;
            this.connectionString = props.getProperty(prefix + "url");
            if (this.connectionString == null || this.connectionString.length() == 0) {
                this.connectionString = "/local";
            }
            this.authMode = parseAuthMode(props.getProperty(prefix + "auth.mode"));
            this.saKeyFile = props.getProperty(prefix + "auth.sakey");
            this.staticLogin = props.getProperty(prefix + "auth.username");
            this.staticPassword = props.getProperty(prefix + "auth.password");
            this.tlsCertificateFile = props.getProperty(prefix + "cafile");
            this.preferLocalDc = Boolean.parseBoolean(
                    props.getProperty(prefix + "preferLocalDc", "false"));
            this.poolSize = parseInt(props.getProperty(prefix + "poolSize"), this.poolSize, "poolSize");
            this.properties.putAll(props);
        }

        private static AuthMode parseAuthMode(String value) {
            if (value == null || value.length() == 0) {
                return AuthMode.NONE;
            }
            try {
                return AuthMode.valueOf(value);
            } catch (IllegalArgumentException iae) {
                throw new RuntimeException("Unsupported authmode: " + value, iae);
            }
        }

        private static int parseInt(String value, int defval, String comment) {
            if (value == null || value.length() == 0) {
                return defval;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Failed to parse " + comment
                        + " as integer, input value: " + value);
            }
        }

        /**
         * Loads a configuration from the specified file. This is a convenience
         * method that delegates to the overloaded version without an explicit
         * property name prefix parameter.
         *
         * @param fname the file path to load the configuration from
         * @return the parsed configuration object
         */
        public static Config fromFile(String fname) {
            return fromFile(fname, null);
        }

        /**
         * Reads and parses a configuration file into a {@link Config} object.
         * The file is read as bytes, then parsed as XML properties. A custom
         * prefix for property names can be applied during configuration
         * processing.
         *
         * @param fname the path to the configuration file
         * @param prefix the custom prefix for property names to be read when
         * constructing the Config object
         * @return a new {@link Config} object loaded with the specified
         * properties
         * @throws RuntimeException if the file cannot be read or parsed
         */
        public static Config fromFile(String fname, String prefix) {
            byte[] data;
            try {
                data = Files.readAllBytes(Paths.get(fname));
            } catch (IOException ix) {
                throw new RuntimeException("Failed to read file " + fname, ix);
            }
            return fromBytes(data, fname, prefix);
        }

        public static Config fromBytes(byte[] data, String fname, String prefix) {
            Properties props = new Properties();
            try {
                props.loadFromXML(new ByteArrayInputStream(data));
            } catch (IOException ix) {
                throw new RuntimeException("Failed to parse properties file " + fname, ix);
            }
            return new Config(props, prefix);
        }

        public String getPrefix() {
            return prefix;
        }

        public String getConnectionString() {
            return connectionString;
        }

        public void setConnectionString(String connectionString) {
            this.connectionString = connectionString;
        }

        public AuthMode getAuthMode() {
            return authMode;
        }

        public void setAuthMode(AuthMode authMode) {
            if (authMode == null) {
                this.authMode = AuthMode.NONE;
            } else {
                this.authMode = authMode;
            }
        }

        public String getSaKeyFile() {
            return saKeyFile;
        }

        public void setSaKeyFile(String saKeyFile) {
            this.saKeyFile = saKeyFile;
        }

        public String getStaticLogin() {
            return staticLogin;
        }

        public void setStaticLogin(String staticLogin) {
            this.staticLogin = staticLogin;
        }

        public String getStaticPassword() {
            return staticPassword;
        }

        public void setStaticPassword(String staticPassword) {
            this.staticPassword = staticPassword;
        }

        public String getTlsCertificateFile() {
            return tlsCertificateFile;
        }

        public void setTlsCertificateFile(String tlsCertificateFile) {
            this.tlsCertificateFile = tlsCertificateFile;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            if (poolSize <= 0) {
                poolSize = 2 * (1 + Runtime.getRuntime().availableProcessors());
            }
            this.poolSize = poolSize;
        }

        public boolean isPreferLocalDc() {
            return preferLocalDc;
        }

        public void setPreferLocalDc(boolean preferLocalDc) {
            this.preferLocalDc = preferLocalDc;
        }

        public Properties getProperties() {
            return properties;
        }

    }

    /**
     * Supported authentication modes for YDB connections.
     */
    public static enum AuthMode {

        /**
         * No authentication.
         */
        NONE,
        /**
         * Authentication via environment variables.
         */
        ENV,
        /**
         * Authentication via static credentials, e.g. login+password.
         */
        STATIC,
        /**
         * Authentication via virtual machine metadata.
         */
        METADATA,
        /**
         * Authentication via service account key file.
         */
        SAKEY

    }
}
