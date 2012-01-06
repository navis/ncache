package com.nexr.cache;

import java.io.IOException;
import java.util.List;
import java.util.TimerTask;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nexr.cache.util.Property;
import com.nexr.cache.util.ZooKeeperUtil;

public abstract class ClusterWatcher<R extends ResourceManager, C extends ConnectionManager, NS extends NameSpace> extends NameSpaceManager<NS> implements Watcher {

    protected final R resource;
    protected final C connection;
    protected final ZooKeeperUtil client;

    public static final int SESSION_TIMEOUT = 3000;
    public static final int RETRY_INTERVAL = 10000;
    public static final int RETRY_INTERVAL_MAX = 30000;
    public static final int REPORT_INTERVAL = 120000;

    public ClusterWatcher(String description, String zookeeper, String config) throws Exception {
        resource = initResources(initProperty(config));
        connection = initConnectionManager(description);
        client = initZookeeper(zookeeper);
    }

    private Property initProperty(String config) throws IOException {
        return new Property(config);
    }

    protected abstract R initResources(Property props) throws Exception;

    protected abstract C initConnectionManager(String description) throws Exception;

    protected abstract ZooKeeperUtil initZookeeper(String zookeeper) throws Exception;

    protected void syncRuntime(final WatchedEvent event, final Watcher watcher) {
        if (event.getType() == Event.EventType.None) {
            System.out.println("[ClusterWatcher/syncRuntime] " + event.getState());
            return;
        }
        System.out.println("[ClusterWatcher/syncRuntime] " + event.getPath() + ", " + event.getType());
        try {
            syncRuntime(client.getChildren(event.getPath(), watcher));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("-- [ClusterWatcher/syncRuntime] failed by " + e.toString());
            scheduleRuntime(event, watcher);
        }
    }

    private void scheduleRuntime(final WatchedEvent event, final Watcher watcher) {
        scheduleTask(new TimerTask() {
            public void run() { syncRuntime(event, watcher); }
        }, RETRY_INTERVAL);
    }

    protected void syncRuntime(List<String> children) throws Exception {
        for (String child : children) {
            ServerEntity server = connection.getServer(child);
            if (server != null && !server.isActive()) {
                server.updateAddress(PartitionUtil.parseAddress(client.getData(connection.serverRuntimePath(child), null)));
                System.out.println("-- [ClusterWatcher/syncRuntime] activated " + server);
                activated(server);
            }
        }
        for (String removed : connection.removedNames(children)) {
            ServerEntity server = connection.getServer(removed);
            if (server != null && server.isActive()) {
                System.out.println("-- [ClusterWatcher/syncRuntime] passivated " + server);
                passivated(server);
            }
        }
    }

    protected void activated(ServerEntity server) {
        server.activate();
        stateChanged(server, true);
    }

    protected void passivated(ServerEntity server) {
        server.passivate();
        stateChanged(server, false);
    }

    protected void stateChanged(ServerEntity server, boolean activated) {
    }

    protected void syncNamespaces(List<String> nspaces) throws Exception {
        for (String nspace : nspaces) {
            syncNamespace(NameSpace.valueOf(this, nspace));
        }
    }

    protected void syncNamespace(WatchedEvent event, Watcher watcher) {
        if (event.getType() == Event.EventType.None) {
            System.out.println("[CacheServer/syncNamespace] " + event.getState());
            return;
        }
        System.out.println("[CacheServer/syncNamespace] " + event.getPath() + ", " + event.getType());
        try {
            syncNamespace(NameSpace.valueOf(this, Utils.lastPath(event.getPath())));
        } catch (Exception e) {
            System.out.println("-- [CacheServer/syncNamespace] failed by " + e.toString());
            scheduleNamespace(event, watcher);
        }
    }

    private void scheduleNamespace(final WatchedEvent event, final Watcher watcher) {
        scheduleTask(new TimerTask() {
            public void run() { syncNamespace(event, watcher); }
        }, RETRY_INTERVAL);
    }

    protected void syncPartitions(Watcher watcher) throws Exception {
        for (NameSpace nspace : indexMap) {
            String path = connection.nspaceRuntimePath(nspace.name());
            syncPartition(nspace.index(), client.getData(path, watcher));
        }
    }

    protected void syncPartition(WatchedEvent event, Watcher watcher) {
        if (event.getType() == Event.EventType.None) {
            System.out.println("[ClusterWatcher/syncPartition] " + event.getState());
            return;
        }
        System.out.println("[ClusterWatcher/syncPartition] " + event.getPath() + ", " + event.getType());
        try {
            String namespace = Utils.lastPath(event.getPath());
            syncPartition(namespace, client.getData(event.getPath(), watcher));
        } catch (Exception e) {
            System.out.println("-- [ClusterWatcher/syncPartition] failed by " + e.toString());
            schedulePartition(event, watcher);
        }
    }

    private void schedulePartition(final WatchedEvent event, final Watcher watcher) {
        scheduleTask(new TimerTask() {
            public void run() { syncPartition(event, watcher); }
        }, RETRY_INTERVAL);
    }

    protected void syncServer(List<String> children) {
        connection.syncServer(children);
    }

    protected void syncServer(final WatchedEvent event, final Watcher watcher) {
        if (event.getType() == Event.EventType.None) {
            System.out.println("[ClusterWatcher/syncEntity] " + event.getState());
            return;
        }
        System.out.println("[ClusterWatcher/syncEntity] " + event.getPath() + ", " + event.getType());
        try {
            syncServer(client.getChildren(event.getPath(), watcher));
        } catch (Exception e) {
            System.out.println("-- [ClusterWatcher/syncEntity] failed by " + e.toString());
            scheduleServer(event, watcher);
        }
    }

    private void scheduleServer(final WatchedEvent event, final Watcher watcher) {
        scheduleTask(new TimerTask() {
            public void run() { syncServer(event, watcher); }
        }, RETRY_INTERVAL);
    }

    public void process(WatchedEvent event) {
        System.out.println("[ClusterWatcher/process] " + event);
        if (event.getState() == Event.KeeperState.Expired) {
            destroy();
        }
    }

    protected void scheduleTask(TimerTask task, long delay) {
        resource.schedule(task, delay);
    }

    public void destroy() {
        client.close();
        resource.destroy();
    }
}
