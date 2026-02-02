package tech.ydb.mv.svc;

import java.time.Duration;
import java.util.HashMap;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.SemaphoreLease;
import tech.ydb.core.Result;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;

/**
 *
 * @author zinal
 */
public class MvLocker implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvLocker.class);

    private final CoordinationClient client;
    private final CoordinationSession session;
    private final String nodePath;
    private final HashMap<String, SemaphoreLease> leases = new HashMap<>();
    private final Duration timeout;

    public MvLocker(YdbConnector conn) {
        this.client = conn.getCoordinationClient();
        this.nodePath = conn.getProperty(MvConfig.CONF_COORD_PATH, MvConfig.DEF_COORD_PATH);
        prepareNode(conn, this.nodePath);
        this.session = obtainSession(conn, this.nodePath);
        int seconds = conn.getProperty(MvConfig.CONF_COORD_TIMEOUT, 10);
        if (seconds < 5) {
            seconds = 5;
        }
        this.timeout = Duration.ofSeconds(seconds);
        LOG.debug("Initialized locking guard {} on `{}` with default timeout {}",
                System.identityHashCode(this), this.nodePath, this.timeout);
    }

    @Override
    public void close() {
        LOG.debug("Shutting down locking guard {} on `{}`",
                System.identityHashCode(this), this.nodePath);
        releaseAll();
        try {
            session.close();
        } catch (Exception ex) {
            LOG.warn("Failed to close the coordination session", ex);
        }
    }

    private static void prepareNode(YdbConnector conn, String nodePath) {
        // QueryRetryContext used for retry processing here,
        // QuerySession is not actually needed
        conn.getQueryRetryCtx().supplyStatus(
                qs -> conn.getCoordinationClient().createNode(nodePath)
        ).join().expectSuccess();
    }

    private static CoordinationSession obtainSession(YdbConnector conn, String nodePath) {
        return conn.getQueryRetryCtx().supplyResult(
                qs -> {
                    var session = conn.getCoordinationClient().createSession(nodePath);
                    return session.connect().thenApplyAsync(
                            status -> status.isSuccess() ? Result.success(session) : Result.fail(status)
                    );
                }
        ).join().getValue();
    }

    public CoordinationClient getClient() {
        return client;
    }

    public CoordinationSession getSession() {
        return session;
    }

    public String getNodePath() {
        return nodePath;
    }

    public void releaseAll() {
        String nextName = null;
        synchronized (leases) {
            if (!leases.isEmpty()) {
                nextName = leases.keySet().iterator().next();
            }
        }
        while (nextName != null) {
            release(nextName);
            synchronized (leases) {
                if (leases.isEmpty()) {
                    nextName = null;
                } else {
                    nextName = leases.keySet().iterator().next();
                }
            }
        }
    }

    public boolean lock(String name) {
        return lock(name, timeout);
    }

    public boolean lock(String name, Duration timeout) {
        name = YdbConnector.safe(name);
        LOG.debug("Ensuring the single `{}` job instance "
                + "through lock with timeout {}...", name, timeout);
        SemaphoreLease lease;
        synchronized (leases) {
            lease = leases.get(name);
        }
        if (lease != null) {
            LOG.debug("Lock `{}` already obtained, moving forward.", name);
            return true;
        }
        try {
            lease = session.acquireEphemeralSemaphore(name, true, timeout)
                    .join().getValue();
        } catch (Exception ex) {
            LOG.debug("Failed to acquire the semaphore {}", name, ex);
            return false;
        }
        synchronized (leases) {
            leases.put(name, lease);
        }
        LOG.info("Lock `{}` obtained.", name);
        return true;
    }

    public boolean release(String name) {
        name = YdbConnector.safe(name);
        SemaphoreLease lease;
        synchronized (leases) {
            lease = leases.remove(name);
        }
        if (lease == null) {
            return false;
        }
        try {
            lease.release().join();
            LOG.info("Lock `{}` released.", name);
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to release the lock `{}`", name, ex);
            return false;
        }
    }

}
