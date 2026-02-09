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
public class MvConfig extends MvConfigBase {

    private String connectionString;
    private MvConfigBase.AuthMode authMode = MvConfigBase.AuthMode.NONE;
    private String saKeyFile;
    private String staticLogin;
    private String staticPassword;
    private String tlsCertificateFile;
    private int poolSize = 2 * (1 + Runtime.getRuntime().availableProcessors());
    private boolean preferLocalDc = false;
    private final String prefix;

    public MvConfig() {
        this.prefix = "ydb.";
        this.connectionString = "/local";
    }

    public MvConfig(Properties props) {
        this(props, null);
    }

    public MvConfig(Properties props, String prefix) {
        super(props);
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
        this.preferLocalDc = super.getProperty(prefix + "preferLocalDc", false);
        this.poolSize = super.getProperty(prefix + "poolSize", this.poolSize);
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

    public MvConfigBase.AuthMode getAuthMode() {
        return authMode;
    }

    public void setAuthMode(MvConfigBase.AuthMode authMode) {
        if (authMode == null) {
            this.authMode = MvConfigBase.AuthMode.NONE;
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
     * Loads a configuration from the specified file. This is a convenience
     * method that delegates to the overloaded version without an explicit
     * property name prefix parameter.
     *
     * @param fname the file path to load the configuration from
     * @return the parsed configuration object
     */
    public static MvConfig fromFile(String fname) {
        return fromFile(fname, null);
    }

    /**
     * Reads and parses a configuration file into a {@link MvConfig}
     * object.The file is read as bytes, then parsed as XML properties.A custom
     * prefix for property names can be applied during configuration processing.
     *
     * @param fname the path to the configuration file
     * @param prefix the custom prefix for property names to be read when
     * constructing the Config object
     * @return a new {@link MvConfig} object loaded with the specified
     * properties
     * @throws RuntimeException if the file cannot be read or parsed
     */
    public static MvConfig fromFile(String fname, String prefix) {
        byte[] data;
        try {
            data = Files.readAllBytes(Paths.get(fname));
        } catch (IOException ix) {
            throw new RuntimeException("Failed to read file " + fname, ix);
        }
        return fromBytes(data, fname, prefix);
    }

    public static MvConfig fromBytes(byte[] data, String fname, String prefix) {
        Properties props = new Properties();
        try {
            props.loadFromXML(new ByteArrayInputStream(data));
        } catch (IOException ix) {
            throw new RuntimeException("Failed to parse properties file " + fname, ix);
        }
        return new MvConfig(props, prefix);
    }

}
