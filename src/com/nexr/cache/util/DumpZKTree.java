package com.nexr.cache.util;

import org.apache.zookeeper.ZooKeeper;

import com.nexr.cache.util.ZooKeeperUtil;

public class DumpZKTree {

    public static void main(String[] args) throws Exception {
        ZooKeeperUtil client = new ZooKeeperUtil(new ZooKeeper(args[0], 3000, null));
        try {
            System.out.println(client.list());
        } finally {
            client.close();
        }
    }
}
