package tech.ydb.mv.svc;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.SemaphoreLease;
import tech.ydb.coordination.settings.DescribeSemaphoreMode;
import tech.ydb.core.Result;
import tech.ydb.query.tools.SessionRetryContext;

import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class MvLocker implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvLocker.class);

    private final SessionRetryContext retryCtx;
    private final CoordinationClient client;
    private final String nodePath;
    private final HashMap<String, Lease> leases = new HashMap<>();
    private final Duration timeout;

    public MvLocker(MvConnector.ConnMgt conn) {
        this.retryCtx = conn.getQueryRetryCtx();
        this.client = conn.getCoordinationClient();
        this.nodePath = conn.getProperty(MvConfig.CONF_COORD_PATH, MvConfig.DEF_COORD_PATH);
        prepareCoordNode(conn, this.nodePath);
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
    }

    private static void prepareCoordNode(MvConnector.ConnMgt conn, String nodePath) {
        // QueryRetryContext used for retry processing here,
        // QuerySession is not actually needed
        conn.getQueryRetryCtx().supplyStatus(
                qs -> conn.getCoordinationClient().createNode(nodePath)
        ).join().expectSuccess();
    }

    public CoordinationClient getClient() {
        return client;
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
        name = MvConfig.safe(name);
        LOG.debug("Ensuring the single `{}` job instance "
                + "through lock with timeout {}...", name, timeout);
        Lease lease;
        synchronized (leases) {
            lease = leases.get(name);
        }
        if (lease != null) {
            LOG.debug("Lock `{}` already obtained, moving forward.", name);
            return true;
        }
        try {
            lease = new Lease(name);
        } catch (Exception ex) {
            LOG.debug("Failed to acquire the semaphore {}", name, ex);
            return false;
        }
        synchronized (leases) {
            leases.put(lease.name, lease);
        }
        LOG.info("Lock `{}` obtained.", name);
        return true;
    }

    public boolean release(String name) {
        name = MvConfig.safe(name);
        Lease lease;
        synchronized (leases) {
            lease = leases.remove(name);
        }
        if (lease == null) {
            return false;
        }
        boolean success = true;
        try {
            lease.semaphore.release().get(10L, TimeUnit.SECONDS);
        } catch (Exception ex) {
            success = false;
            LOG.warn("Failed to release the lock `{}`", name, ex);
        }
        try {
            lease.session.stop().get(10L, TimeUnit.SECONDS);
        } catch (Exception ex) {
            success = false;
            LOG.warn("Failed to close the session for lock `{}`", name, ex);
        }
        LOG.info("Lock `{}` released.", name);
        return success;
    }

    public boolean check(String name) {
        name = MvConfig.safe(name);
        Lease lease;
        synchronized (leases) {
            lease = leases.get(name);
        }
        if (lease == null) {
            return false;
        }
        return lease.check();
    }

    private CoordinationSession obtainSession() {
        return retryCtx.supplyResult(
                qs -> {
                    var session = client.createSession(nodePath);
                    return session.connect().thenApplyAsync(
                            status -> status.isSuccess() ? Result.success(session) : Result.fail(status)
                    );
                }
        ).join().getValue();
    }

    private class Lease {

        final String name;
        final CoordinationSession session;
        final SemaphoreLease semaphore;

        Lease(String name) {
            this.name = name;
            this.session = obtainSession();
            try {
                this.semaphore = this.session
                        .acquireEphemeralSemaphore(name, true, timeout)
                        .join().getValue();
            } catch (Exception ex) {
                this.session.close();
                throw new RuntimeException("Failed to obtain lock " + name, ex);
            }
        }

        boolean check() {
            switch (session.getState()) {
                case LOST:
                case CLOSED:
                    return false;
                default:
                    ;
            }
            var result = session.describeSemaphore(name, DescribeSemaphoreMode.WITH_OWNERS).join();
            if (!result.isSuccess()) {
                return false;
            }
            for (var owner : result.getValue().getOwnersList()) {
                if (owner.getId() == session.getId()) {
                    return true;
                }
            }
            return false;
        }

    }

}
