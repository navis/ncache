package com.nexr.cache.client;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nexr.cache.ClusterWatcher;
import com.nexr.cache.NameSpace;
import com.nexr.cache.ServerEntity;
import com.nexr.cache.message.ClientPush;
import com.nexr.cache.message.ClientRetrieve;
import com.nexr.cache.message.Get;
import com.nexr.cache.message.Message;
import com.nexr.cache.message.Remove;
import com.nexr.cache.message.request.RequestMultiple;
import com.nexr.cache.util.Property;
import com.nexr.cache.util.ZooKeeperUtil;

public class ServiceHandler extends ClusterWatcher<ClientResource, ClientConnectionManager, NameSpace> implements CacheClient {

    public static final int SESSION_TIMEOUT = 3000;
    public static final int RETRY_INTERVAL = 10000;

    private NameSpace nspace;

    public ServiceHandler(String expression, String zookeeper) throws Exception {
        super(expression, zookeeper, null);
    }

    protected ClientResource initResources(Property props) throws Exception {
        ClientResource client = new ClientResource();
        client.initialize(props);
        return client;
    }

    protected ClientConnectionManager initConnectionManager(String expression) {
        return new ClientConnectionManager(expression, resource);
    }

    protected ZooKeeperUtil initZookeeper(String zookeeper) throws Exception {
        return new ZooKeeperUtil(new ZooKeeper(zookeeper, SESSION_TIMEOUT, this));
    }

    public void initialize() throws Exception {
        syncServer(client.getChildren(connection.serverConfigPath(), new Watcher() {
            public void process(WatchedEvent event) { syncServer(event, this); }
        }));
        syncRuntime(client.getChildren(connection.serverRuntimePath(), new Watcher() {
            public void process(WatchedEvent event) { syncRuntime(event, this); }
        }));
        syncNamespaces(client.getChildren(connection.nspaceConfigPath(), new Watcher() {
            public void process(WatchedEvent event) { syncNamespace(event, this); }
        }));
        syncPartitions(new Watcher() {
            public void process(WatchedEvent event) { syncPartition(event, this); }
        });
    }

    public byte[] get(String key) throws IOException {
        return get(key, DEFAULT_TIMEOUT);
    }

    public byte[] get(String key, long timeout) throws IOException {
        NameSpace nspace = namespace();
        Get get = new Get(key.getBytes(), nspace.index(), timeout);
        int[] partitions = nspace.partitionForHash(get.hash);
        RequestMultiple request = new RequestMultiple(resource.nextCallID(), partitions.length);

        long prev = System.currentTimeMillis();
        for (int partition : partitions) {
            ServerEntity target = connection.serverFor(partition);
            if (!target.isActive()) {
                request.skipped(partition);
                continue;
            }
            System.out.println("[ConnectionManager/get] " + target);
            Message retrieved = target.request(get, request).await(DEAULT_GET_FINDNEXT);
            if (retrieved != null) {
                get.cancelRequest();
                return retrieved instanceof ClientRetrieve ? ((ClientRetrieve) retrieved).value : null;
            }
            long current = System.currentTimeMillis();
            timeout -= current - prev;
            prev = current;
        }
        while (timeout > 0 && request.remaining()) {
            Message retrieved = request.await(timeout);
            if (retrieved != null) {
                get.cancelRequest();
                return retrieved instanceof ClientRetrieve ? ((ClientRetrieve) retrieved).value : null;
            }
            long current = System.currentTimeMillis();
            timeout -= current - prev;
            prev = current;
        }
        return null;
    }

    public boolean put(String key, byte[] value) throws IOException {
        return put(key, value, DEFAULT_EXPIRE, DEFAULT_PERSIST, DEFAULT_TIMEOUT);
    }

    public boolean put(String key, byte[] value, int expire) throws IOException {
        return put(key, value, expire, DEFAULT_PERSIST, DEFAULT_TIMEOUT);
    }

    public boolean put(String key, byte[] value, boolean persist) throws IOException {
        return put(key, value, DEFAULT_EXPIRE, persist, DEFAULT_TIMEOUT);
    }

    public boolean put(String key, byte[] value, int expire, boolean persist, long timeout) throws IOException {
        NameSpace nspace = namespace();
        ClientPush put = new ClientPush(key.getBytes(), value, expire, persist, nspace.index(), timeout);
        for (int sindex : nspace.partitionForHash(put.hash)) {
            ServerEntity target = connection.serverFor(sindex);
            if (target.isConnected()) {
                Message message = target.request(put).await(DEAULT_PUT_FINDNEXT);
                if (message != null) {
                    put.cancelRequest();
                    return message.opcode() == Message.ACK;
                }
            }
        }
        return false;
    }

    public boolean remove(String key) throws IOException {
        NameSpace nspace = namespace();
        Remove remove = new Remove(key.getBytes(), nspace.index());
        for (int sindex : nspace.partitionForHash(remove.hash)) {
            ServerEntity target = connection.serverFor(sindex);
            if (target.isActive()) {
                Message message = target.request(remove).await(DEFAULT_TIMEOUT);
                return message != null && message.opcode() == Message.ACK;
            }
        }
        return false;
    }

    private NameSpace namespace() {
        return nspace != null ? nspace : defaultNS();
    }

    public void useNamespace(String namespace) {
        NameSpace nspace = namespaceFor(namespace);
        if (nspace == null) {
            throw new IllegalArgumentException(namespace + " is not registered");
        }
        this.nspace = nspace;
    }

    public void flushAll() throws IOException {
    }

    public void shutdown() {
        destroy();
    }

    public static void main(String[] args) throws Exception {
        ServiceHandler manager = new ServiceHandler(args[0], args[1]);
        manager.initialize();

        try {
            byte[] value = manager.get("navis", 3000);
            if (value != null) {
                System.out.println("-- [ConnectionManager/main] " + new String(value));
            }
            manager.put("navis", "navis manse".getBytes(), DEFAULT_EXPIRE, true, DEFAULT_TIMEOUT);

            value = manager.get("navis", 3000);
            System.out.println("-- [ConnectionManager/main] " + new String(value));

            if (manager.remove("navis")) {
                System.out.println("-- [ConnectionManager/main] " + new String(value));
            }
        } finally {
            manager.shutdown();
        }
    }
}
