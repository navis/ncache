package com.nexr.cache.util;

import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import org.apache.zookeeper.ZooKeeper;

import com.nexr.cache.NameSpace;
import com.nexr.cache.NameSpaceManager;
import com.nexr.cache.Names;
import static com.nexr.cache.Names.CONFIG;
import static com.nexr.cache.Names.RUNTIME;
import com.nexr.cache.PartitionUtil;
import com.nexr.cache.IndexedServerName;

public class CreateCluster {

    // zk-address cluster-name [ns:max:replica:persist]
    public static void main(String[] args) throws Exception {
        ZooKeeperUtil client = new ZooKeeperUtil(new ZooKeeper(args[0], 3000, null));
        int index0 = args[1].indexOf('=');
        String cluster = args[1].substring(0, index0).trim();
        String[] servers = args[1].substring(index0 + 1).split("[ ]?,[ ]?");

        client.ensurePath(CONFIG, PERSISTENT);
        client.ensurePath(RUNTIME, PERSISTENT);
        client.ensurePath(Names.configPath(cluster), PERSISTENT);
        client.ensurePath(Names.runtimePath(cluster), PERSISTENT);
        client.ensurePath(Names.serverConfigPath(cluster), PERSISTENT);
        client.ensurePath(Names.nspaceConfigPath(cluster), PERSISTENT);
        client.ensurePath(Names.serverRuntimePath(cluster), PERSISTENT);
        client.ensurePath(Names.nspaceRuntimePath(cluster), PERSISTENT);

        int counter = 0;
        for (String server : servers) {
            client.ensurePath(Names.configPath(cluster, new IndexedServerName(counter++, server)), PERSISTENT);
        }

        NameSpaceManager<NameSpace> manager = new NameSpaceManager<NameSpace>();

        List<NameSpace> namespaces = new ArrayList<NameSpace>();
        for (int i = 2; i < args.length; i++) {
            namespaces.add(NameSpace.valueOf(manager, args[i]));
        }
        if (namespaces.isEmpty()) {
            namespaces.add(NameSpace.defaultNameSpace());
        }
        for (NameSpace nspace : namespaces) {
            client.ensurePath(Names.configPath(cluster, nspace), PERSISTENT);
            client.ensurePath(Names.nspaceRuntimePath(cluster, nspace.name()), PERSISTENT);

            int[][] partition = PartitionUtil.newPartition(271, nspace.replicaNum(), servers.length);
            client.setData(Names.nspaceRuntimePath(cluster, nspace.name()), PartitionUtil.partitionToBytes(partition));
        }

        client.close();
    }
}
