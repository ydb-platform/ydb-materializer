package tech.ydb.mv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Configuration class for YDB database connections. It holds various properties
 * for connection strings, authentication settings, TLS certificate files,
 * connection pool size, and a prefix used for property lookups. The
 * configuration can be initialized with or without a set of Java Properties,
 * optionally using a custom prefix for property names. A static method provided
 * by the class allows loading configuration from an external XML properties
 * file.
 */
public class MvConfig extends MvName {

    private String connectionString;
    private MvName.AuthMode authMode = MvName.AuthMode.NONE;
    private String saKeyFile;
    private String staticLogin;
    private String staticPassword;
    private String tlsCertificateFile;
    private int poolSize = 2 * (1 + Runtime.getRuntime().availableProcessors());
    private boolean preferLocalDc = false;

    private final Properties properties = new Properties();

    public MvConfig() {
        this.connectionString = "/local";
    }

    public MvConfig(Properties properties) {
        this(properties, null);
    }

    public MvConfig(Properties p, String prefix) {
        if (prefix == null) {
            prefix = "";
        } else {
            prefix = prefix.trim();
            if (prefix.length() > 0 && !prefix.endsWith(".")) {
                prefix = prefix + ".";
            }
        }
        this.properties.putAll(p);
        this.connectionString = p.getProperty(prefix + MvName.CONF_YDB_URL);
        if (this.connectionString == null || this.connectionString.length() == 0) {
            this.connectionString = "/local";
        }
        this.authMode = parseAuthMode(p.getProperty(prefix + MvName.CONF_YDB_AUTH_MODE));
        this.saKeyFile = p.getProperty(prefix + MvName.CONF_YDB_AUTH_SAKEY);
        this.staticLogin = p.getProperty(prefix + MvName.CONF_YDB_AUTH_USERNAME);
        this.staticPassword = p.getProperty(prefix + MvName.CONF_YDB_AUTH_PASSWORD);
        this.tlsCertificateFile = p.getProperty(prefix + MvName.CONF_YDB_CAFILE);
        this.preferLocalDc = getProperty(prefix + MvName.CONF_YDB_PREFER_LOCAL_DC, false);
        this.poolSize = getProperty(prefix + MvName.CONF_YDB_POOL_SIZE, this.poolSize);
    }

    public final Properties getProperties() {
        return properties;
    }

    public final String getProperty(String name) {
        return properties.getProperty(name);
    }

    public final String getProperty(String name, String defval) {
        return properties.getProperty(name, defval);
    }

    public final int getProperty(String name, int defval) {
        return parseInt(properties, name, defval);
    }

    public final long getProperty(String name, long defval) {
        return parseLong(properties, name, defval);
    }

    public final boolean getProperty(String name, boolean defval) {
        String v = properties.getProperty(name, String.valueOf(defval));
        return Boolean.parseBoolean(v);
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public MvName.AuthMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(MvName.AuthMode authMode) {
        if (authMode == null) {
            this.authMode = MvName.AuthMode.NONE;
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

    /**
     * Reads and parses a configuration file into a {@link MvConfig} object.The
     * file is read as bytes, then parsed as XML properties.A custom prefix for
     * property names can be applied during configuration processing.
     *
     * @param fname the path to the configuration file
     * @return a new {@link MvConfig} object loaded with the specified
     * properties
     * @throws RuntimeException if the file cannot be read or parsed
     */
    public static MvConfig fromFile(String fname) {
        byte[] data;
        try {
            data = Files.readAllBytes(Paths.get(fname));
        } catch (IOException ix) {
            throw new RuntimeException("Failed to read file " + fname, ix);
        }
        return fromBytes(data, fname);
    }

    /**
     * Reads and parses configuration data into a {@link MvConfig} object.
     *
     * @param data Configuration data as XML
     * @param fname Filename, from which the data has been obtained
     * @return Configuration object
     */
    public static MvConfig fromBytes(byte[] data, String fname) {
        Properties props = new Properties();
        try {
            props.loadFromXML(new ByteArrayInputStream(data));
        } catch (IOException ix) {
            throw new RuntimeException("Failed to parse properties file " + fname, ix);
        }
        return new MvConfig(props);
    }

    /**
     * Reads and parses configuration data into a {@link MvConfig} object.
     *
     * @param data Configuration data as XML
     * @return Configuration object
     */
    public static MvConfig fromBytes(byte[] data) {
        return fromBytes(data, "config.xml");
    }

}
