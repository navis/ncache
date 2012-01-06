package com.nexr.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nexr.cache.message.*;
import com.nexr.cache.message.request.RequestAggregation;
import com.nexr.cache.util.Generations;
import com.nexr.cache.util.Property;
import com.nexr.cache.util.ZooKeeperUtil;

public class CacheServer extends ClusterWatcher<ServerResource, ServerConnectionManager, ServerNameSpace> implements MessageHandler {

    ServiceListener listener;

    public CacheServer(String name, String zookeeper, String config) throws Exception {
        super(name, zookeeper, config);
        resource.initialized(connection, this);
    }

    protected ServerResource initResources(Property props) throws Exception {
        ServerResource resource = new CacheServerResource();
        resource.initialize(props);
        return resource;
    }

    protected ServerConnectionManager initConnectionManager(String expression) {
        return new ServerConnectionManager(new FullName(expression), resource);
    }

    protected ZooKeeperUtil initZookeeper(String zookeeper) throws Exception {
        ZooKeeperUtil client = new ZooKeeperUtil(new ZooKeeper(zookeeper, SESSION_TIMEOUT, this));
        client.ensurePath(Names.MANAGER, PERSISTENT);
        client.ensurePath(Names.CONFIG, PERSISTENT);
        client.ensurePath(Names.RUNTIME, PERSISTENT);
        return client;
    }

    public void start(String address) throws Exception {
        try {
            startCluster(address);
            startAcceptor();
        } catch (Exception e) {
            destroy();
            throw e;
        }
        if (!connection.isManager()) {
            startReporter();
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    private void startReporter() {
        resource.schedule(new Reporter(), REPORT_INTERVAL, REPORT_INTERVAL);
    }

    private void startAcceptor() throws IOException {
        ServerEntity server = connection.self();
        String accept = server.addressFor(Constants.NCACHE_ADDRESS);
        listener = new ServiceListener(resource, accept).start();
        synchronizeCluster(resource);
        listener.acceptClients();
    }

    private ServerEntity startCluster(String address) throws Exception {
        ServerEntity current = connection.initEntity(client.getChildren(connection.serverConfigPath(), new Watcher() {
            public void process(WatchedEvent event) { syncServer(event, this); }
        }));
        if (current == null) {
            throw new IllegalStateException("not registered server name " + connection.name());
        }
        syncManager(current.name());
        syncRuntime(client.getChildren(connection.serverRuntimePath(), new Watcher() {
            public void process(WatchedEvent event) { syncRuntime(event, this); }
        }));

        register(current.name(), address);

        syncNamespaces(client.getChildren(connection.nspaceConfigPath(), new Watcher() {
            public void process(WatchedEvent event) { syncNamespace(event, this); }
        }));
        syncPartitions(new Watcher() {
            public void process(WatchedEvent event) { syncPartition(event, this); }
        });
        return current;
    }

    @Override
    protected ServerNameSpace initialize(ServerNameSpace namespace) throws IOException {
        namespace.initialize(resource);
        return namespace;
    }

    private void register(String sname, String address) throws InterruptedException {
        int interval = 2000;
        while (true) {
            ServerEntity server = tryRegister(sname, address);
            if (server != null) {
                return;
            }
            System.out.println("[CacheServer/register] sleeping.. " + interval + " msec");
            Thread.sleep(interval);
            interval = Math.min(RETRY_INTERVAL_MAX, (interval *= 1.4));
        }
    }

    private ServerEntity tryRegister(String sname, String address) {
        System.out.println("-- [CacheServer/tryRegister] " + sname + " --> " + address);
        boolean registered = client.createEphemeral(connection.serverRuntimePath(sname), address.getBytes());
        if (registered) {
            ServerEntity server = connection.getServer(sname);
            server.updateAddress(PartitionUtil.parseAddress(address));
            return server;
        }
        return null;
    }

    protected synchronized ServerEntity syncManager(final String sname) {
        if (client.createEphemeral(connection.managerPath(), sname.getBytes())) {
            return connection.setManager(sname);
        }
        try {
            byte[] value = client.getData(connection.managerPath(), new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getType() != Event.EventType.None) {
                        syncManager(sname);
                    }
                }
            });
            boolean wasManager = connection.isManager();
            ServerEntity server = connection.setManager(new String(value));
            if (wasManager && !connection.isManager()) {
                startReporter();
            }
            return server;
        } catch (Exception e) {
            resource.schedule(new TimerTask() {
                public void run() { syncManager(sname); }
            }, RETRY_INTERVAL);
        }
        return null;
    }

    @Override
    public void destroy() {
        if (listener != null) {
            listener.shutdown();
        }
        super.destroy();
    }

    private class Reporter extends TimerTask {

        public void run() {
            if (connection.isManager()) {
                cancel();
            } else {
                reportTo(connection.manager());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        CacheServer server = new CacheServer(args[0], args[1], args[2]);
        server.start(args[3]);
    }

    @Override
    protected ServerNameSpace[] newArray(int length) {
        return new ServerNameSpace[length];
    }

    @Override
    protected ServerNameSpace newInstance(byte index, String name, int limitKB, int replica, int partition, boolean persist) {
        return new ServerNameSpace(index, name, limitKB, replica, partition, persist);
    }

    // namespace : partition : replica
    private void synchronizeCluster(ServerResource resource) throws IOException {

        ServerConnectionManager cluster = resource.cluster();
        RecoveryListener listener = recoveryListener();

        Snapshot snapshot = new Snapshot(snapshots());
        RequestAggregation request = new RequestAggregation(resource.nextCallID(), listener);
        for (ServerEntity server : cluster.servers()) {
            if (!cluster.isSelf(server) && server.isActive()) {
                request.increment();
                server.request(snapshot, request);
            }
        }
        System.out.println("-- [NameSpaceManager/synchronize] awaits " + request.awaitingOn() + " server responses..");
        listener.requestSync(request);
    }

    protected RecoveryListener recoveryListener() {
        long[][][] elected = new long[namespaceNum()][][];
        for (int i = 0; i < elected.length; i++) {
            elected[i] = nspaceFor(i).recovery();
        }
        return new RecoveryListener(elected);
    }

    public long[][][] snapshots() {
        long[][][] snapshot = new long[namespaceNum()][][];
        for (int namespace = 0; namespace < snapshot.length; namespace++) {
            snapshot[namespace] = cacheFor(namespace).snapshot();
        }
        return snapshot;
    }

    public int[][][] usages() {
        int[][][] usages = new int[namespaceNum()][][];
        for (int i = 0; i < usages.length; i++) {
            usages[i] = cacheFor(i).usages();
        }
        return usages;
    }

    private PartitionedCache cacheFor(int namespace) {
        return nspaceFor(namespace).cache();
    }

    private synchronized ServerNameSpace nspaceFor(int namespace) {
        try {
            return indexMap[namespace];
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid namespace index " + namespace);
        }
    }

    private synchronized int namespaceNum() {
        return indexMap.length;
    }

    public Message handle(Message request) {
        System.out.println("-- [CacheHandler/handle] " + request);
        try {
            switch (request.opcode()) {
                case Message.REPORT:
                    return new Ack();
                case Message.REQ_GET:
                    return handleGet((Get) request);
                case Message.REQ_STORE:
                    return handlePush((ClientPushed) request);
                case Message.FWD_STORE:
                    return handleStoreFoward((StoreForwarded) request);
                case Message.REQ_REMOVE:
                    return handleRemove((Remove) request);
                case Message.FWD_REMOVE:
                    return handleRemoveFoward((RemoveForwarded) request);
                case Message.REQ_FLUSH:
                    return handleFlush((Flush) request);
                case Message.SNAPSHOT:
                    return handleSnapshot((Snapshot) request);
                case Message.SYNC_REQS:
                    return handleSyncRequest((SyncRequests) request);
                case Message.SYNC_DATA:
                    return handleSyncData((SyncPushed) request);
                case Message.SYNC_COMPLETED:
                    return handleSyncCompleted((SyncCompleted) request);
                case Message.CONFLICTED:
                    return handleConflicted((Desync) request);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new Nack(e.toString());
        }
        return null;
    }

    private Message handlePush(ClientPushed request) {
        cacheFor(request.nspace).store(request);
        return new Ack();
    }

    private Message handleGet(Get request) {
        int[] index = cacheFor(request.nspace).retrieve(request.key, request.hash);
        if (index == null) {
            return new Nack("not available");
        }
        return new ServerRetrieve(request.key.length, index);
    }

    private Message handleRemove(Remove remove) {
        cacheFor(remove.nspace).remove(remove);
        return new Ack();
    }

    private Message handleRemoveFoward(final RemoveForwarded request) {
        resource.execute(new Runnable() {
            public void run() { cacheFor(request.nspace).remove(request); }
        });
        return null;
    }

    private Message handleStoreFoward(final StoreForwarded request) {
        resource.execute(new Runnable() {
            public void run() { cacheFor(request.nspace).store(request); }
        });
        return null;
    }

    private Message handleFlush(Flush request) {
        cacheFor(request.nspace).flushAll();
        return new Ack();
    }

    private Message handleSnapshot(Snapshot request) {
        long[][][] snapshot = request.snapshot();
        for (byte namespace = 0; namespace < snapshot.length; namespace++) {
            SyncRequests collector = new SyncRequests(indexMap.length);
            cacheFor(namespace).request(snapshot[namespace], collector);
            if (!collector.isEmpty()) {
                request.source().notify(collector);
            }
        }
        return new Snapshot(snapshots());
    }

    private Message handleSyncRequest(SyncRequests request) {
        int[][][] requests = request.requests();
        for (byte namespace = 0; namespace < requests.length; namespace++) {
            DataCollector collector = new DataCollector(namespace, request.source());
            cacheFor(namespace).response(requests[namespace], collector);
        }
        return null;
    }

    private Message handleSyncData(SyncPushed request) {
        cacheFor(request.nspace).sync(request);
        return null;
    }

    private Message handleSyncCompleted(SyncCompleted request) {
        cacheFor(request.nspace).recovered(request.partition());
        return null;
    }

    private Message handleConflicted(Desync desync) {
        for (Desync.Entry entry : desync.entries) {
            ServerNameSpace nspace = namespaceFor(entry.nspace);
            long recover = nspace.cache().recoveryTarget(desync.partition, pindex, remote[pindex]);
            if (recover >= 0) {
//                int sindex = nspace.partitionFor(conflicted.partition, pindex);
//                requests.add(Generations.INDEX(sindex, recover, partition));
//                System.out.println("[RecoveryListener/requestSync] " + sindex + ":" + partition + ":" + recover + " ~ " + Generations.COUNTER(generation));
            }
        }
        return null;
    }

    public void reportTo(ServerEntity manager) {
        for (ServerNameSpace namespace : indexMap) {
            StatusReport report = new StatusReport(namespace.index(), namespace.usage());
            manager.notify(report);
        }
    }

    @Override
    protected void stateChanged(ServerEntity server, boolean activated) {
        for (ServerNameSpace nameSpace : nspaceArray()) {
            if (nameSpace != null) {
                nameSpace.stateChanged(server.index(), activated);
            }
        }
    }

    class RecoveryListener implements MessageListener {

        final long[][][] results;
        final BlockingQueue<ServerEntity> arrived;

        public RecoveryListener(long[][][] results) {
            this.results = results;
            this.arrived = new LinkedBlockingQueue<ServerEntity>();
        }

        public void arrived(Message message) {
            Snapshot snapshot = (Snapshot) message;
            ServerEntity server = snapshot.source();
            System.out.println("[RecoveryListener/arrived] " + server.index() + ":" + server.name());

            long[][][] remotes = snapshot.snapshot();
            for (int nspace = 0; nspace < remotes.length; nspace++) {
                PartitionedCache cache = cacheFor(nspace);
                System.out.print(Generations.DUMP_ALL(remotes[nspace]));
                for (int partition = 0; partition < remotes[nspace].length; partition++) {
                    long[] partitioned = remotes[nspace][partition];
                    for (int pindex = 0; pindex < partitioned.length; pindex++) {
                        if (partitioned[pindex] == 0) {
                            continue;
                        }
                        long current = results[nspace][partition][pindex];
                        long elected = current != 0 ? Generations.COUNTER(current) : cache.generationFor(partition, pindex);
                        System.out.println("[RecoveryListener/arrived] " + partition + "::" + pindex + " --> " + partitioned[pindex] + "(" + elected + ")");
                        if (partitioned[pindex] > elected) {
                            System.out.println("[RecoveryListener/arrived] elected " + partition + "::" + pindex + "=" + server.index() + ":" + partitioned[pindex]);
                            results[nspace][partition][pindex] = Generations.NEW(server.index(), partitioned[pindex]);
                        } else if (partitioned[pindex] == elected) {
                            if (server.index() == namespaceFor(nspace).partitionFor(partition, pindex)) {
                                System.out.println("[RecoveryListener/arrived] elected " + partition + "::" + pindex + "=" + server.index() + ":" + partitioned[pindex]);
                                results[nspace][partition][pindex] = Generations.NEW(server.index(), partitioned[pindex]);
                            }
                        }
                    }
                }
            }
            arrived.add(snapshot.source());
        }

        public void requestSync(RequestAggregation sync) throws IOException {
            try {
                while (!sync.arrived()) {
                    ServerEntity server = arrived.take();
                    SyncRequests request = new SyncRequests(results.length);
                    for (int nspace = 0; nspace < results.length; nspace++) {
                        List<int[]> requests = new ArrayList<int[]>();
                        for (int partition = 0; partition < results[nspace].length; partition++) {
                            for (int pindex = 0; pindex < results[nspace][partition].length; pindex++) {
                                long generation = results[nspace][partition][pindex];
                                if (generation != 0 && Generations.SERVER(generation) == server.index()) {
                                    long recover = cacheFor(nspace).recoveryTarget(partition, pindex, Generations.COUNTER(generation));
                                    if (recover >= 0) {
                                        int sindex = namespaceFor(nspace).partitionFor(partition, pindex);
                                        requests.add(Generations.INDEX(sindex, recover, partition));
                                        System.out.println("[RecoveryListener/requestSync] " + sindex + ":" + partition + ":" + recover + " ~ " + Generations.COUNTER(generation));
                                    }
                                }
                            }
                        }
                        request.add(nspace, requests);
                    }
                    if (!request.isEmpty()) {
                        server.notify(request);
                    }
                }
            } catch (InterruptedException e) {
                Message message = sync.await(Constants.INITIAL_SYNC_TIMEOUT);
                if (!(message instanceof Ack)) {
                    throw new IOException("failed to receive status");
                }
            }
        }
    }
}
