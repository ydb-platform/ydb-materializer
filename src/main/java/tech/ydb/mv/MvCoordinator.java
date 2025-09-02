package tech.ydb.mv;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.CoordinationSession;
import tech.ydb.coordination.SemaphoreLease;

/**
 *
 * @author zinal
 */
public class MvCoordinator {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinator.class);

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10L);

    private final CoordinationClient client;
    private final CoordinationSession session;
    private final String nodePath;
    private final HashMap<String, SemaphoreLease> leases = new HashMap<>();

    public MvCoordinator(YdbConnector connector) {
        this.client = connector.getCoordinationClient();
        this.nodePath = connector.getProperty(MvConfig.CONF_COORD_PATH, MvConfig.DEF_COORD_PATH);
        this.session = connector.getCoordinationClient().createSession(this.nodePath);
        this.session.connect().join();
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
        ArrayList<String> names = new ArrayList<>();
        synchronized(leases) {
            names.addAll(leases.keySet());
        }
        names.forEach(name -> release(name));
        try {
            session.close();
        } catch(Exception ex) {
            LOG.warn("Failed to close the coordination session", ex);
        }
    }

    public boolean lock(String name) {
        return lock(name, DEFAULT_TIMEOUT);
    }

    public boolean lock(String name, Duration timeout) {
        SemaphoreLease lease;
        synchronized(leases) {
            lease = leases.get(name);
        }
        if (lease!=null) {
            return true;
        }
        try {
            lease = session.acquireEphemeralSemaphore(name, true, timeout).join().getValue();
        } catch(Exception ex) {
            LOG.info("Failed to acquire the semaphore {}: {}", name, ex.toString());
            LOG.debug("Failed to acquire the semaphore {}", name, ex);
            return false;
        }
        synchronized(leases) {
            leases.put(name, lease);
        }
        return true;
    }

    public boolean release(String name) {
        SemaphoreLease lease;
        synchronized(leases) {
            lease = leases.remove(name);
        }
        if (lease==null) {
            return false;
        }
        try {
            lease.release().join();
        } catch(Exception ex) {
            LOG.warn("Failed to release the semaphore {}: {}", name, ex);
        }
        return true;
    }

}
