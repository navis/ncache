package com.nexr.cache.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.nexr.cache.Names;
import com.nexr.cache.IndexedServerName;
import com.nexr.cache.PartitionUtil;

public class SystemManager {

    public static void main(String[] args) throws Exception {
        ZooKeeperUtil client = new ZooKeeperUtil(args[0]);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            executeCommand(line, client);
        }
    }

    private static void executeCommand(String line, ZooKeeperUtil client) {
        try {
            int index0 = line.indexOf(' ');
            String command = line.substring(0, index0).trim();
            if (command.equals("show")) {
                String param1 = line.substring(index0 + 1).trim();
                if (param1.equals("all")) {
                    for (String cluster : client.getChildren(Names.CONFIG)) {
                        showCluster(cluster, client);
                    }
                } else {
                    showCluster(param1, client);
                }
            } else if (command.equals("add")) {
                int index1 = line.indexOf(' ', index0 + 1);
                String cluster = line.substring(index0 + 1, index1).trim();
                client.createPath(Names.serverConfigPath(cluster), CreateMode.PERSISTENT);
                client.createPath(Names.nspaceConfigPath(cluster), CreateMode.PERSISTENT);

                int index = 0;
                String remain = line.substring(index1 + 1);
                for (String server : remain.split("[ ]?,[ ]?")) {
                    IndexedServerName serverName = new IndexedServerName(index++, server);
                    client.createPath(Names.configPath(cluster, serverName), CreateMode.PERSISTENT);
                }
                int[][] partition = PartitionUtil.newPartition(271, 2, index);
                client.ensurePath(Names.runtimePath(cluster), CreateMode.PERSISTENT, PartitionUtil.partitionToBytes(partition));
            } else if (command.equals("remove")) {
                String cluster = line.substring(index0 + 1).trim();
                deleteRecursive(client, Names.configPath(cluster));
                deleteRecursive(client, Names.runtimePath(cluster));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteRecursive(ZooKeeperUtil client, String path) throws Exception {
        for (String child : client.checkChildren(path)) {
            deleteRecursive(client, path + "/" + child);
        }
        client.delete(path);
    }

    private static void showCluster(String cluster, ZooKeeperUtil client) throws KeeperException, InterruptedException {
        System.out.println(cluster);
        for (String child : client.getChildren(Names.serverConfigPath(cluster))) {
            IndexedServerName name = PartitionUtil.parseServerName(child);
            System.out.println("-- " + name);
        }
    }
}
