package com.nexr.cache.util;

import java.util.List;
import java.util.Collections;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import static org.apache.zookeeper.KeeperException.Code.NONODE;
import org.apache.zookeeper.Watcher;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperUtil {

    ZooKeeper client;

    public ZooKeeperUtil(String address) throws IOException {
        client = new ZooKeeper(address, 30000, null);
    }

    public ZooKeeperUtil(ZooKeeper client) {
        this.client = client;
    }

    public void registerWatcher(Watcher watcher) {
        client.register(watcher);
    }

    public boolean watch(String path, Watcher watcher) {
        try {
            client.exists(path, watcher);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean watchChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return client.getChildren(path, watcher) != null;
    }

    public void ensurePath(String pathname, CreateMode mode) throws KeeperException, InterruptedException {
        ensurePath(pathname, mode, null, null);
    }

    public void ensurePath(String pathname, CreateMode mode, Watcher watcher) throws KeeperException, InterruptedException {
        ensurePath(pathname, mode, null, watcher);
    }

    public void ensurePath(String pathname, CreateMode mode, byte[] value) throws KeeperException, InterruptedException {
        ensurePath(pathname, mode, value, null);
    }

    public void ensurePath(String pathname, CreateMode mode, byte[] value, Watcher watcher) throws KeeperException, InterruptedException {
        System.out.println("[ZooKeeperUtil/ensurePath] " + pathname + (value == null ? "" : ", " + value.length + " bytes"));
        try {
            client.create(pathname, value, OPEN_ACL_UNSAFE, mode);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
            if (value != null) {
                client.setData(pathname, value, -1);
            }
        }
        if (watcher != null && client.exists(pathname, watcher) == null) {
            throw KeeperException.create(NONODE, pathname);
        }
    }

    public void setData(String pathname, byte[] value) throws KeeperException, InterruptedException {
        client.setData(pathname, value, -1);
    }

    public boolean createEphemeral(String pathname, byte[] value) {
        try {
            return createPath(pathname, CreateMode.EPHEMERAL, value) != null;
        } catch (Exception e) {
            // failed
        }
        return false;
    }

    public String createPath(String pathname, CreateMode mode) throws KeeperException, InterruptedException {
        return createPath(pathname, mode, null, null);
    }

    public String createPath(String pathname, CreateMode mode, Watcher watcher) throws KeeperException, InterruptedException {
        return createPath(pathname, mode, null, watcher);
    }

    public String createPath(String pathname, CreateMode mode, byte[] value) throws KeeperException, InterruptedException {
        return createPath(pathname, mode, value, null);
    }

    public String createPath(String pathname, CreateMode mode, byte[] value, Watcher watcher) throws KeeperException, InterruptedException {
        String path = client.create(pathname, value, OPEN_ACL_UNSAFE, mode);
        if (watcher != null && client.exists(pathname, watcher) == null) {
            throw KeeperException.create(NONODE, pathname);
        }
        return path;
    }

    public void createPath(String pathname, CreateMode mode, byte[] value, boolean watch) throws KeeperException, InterruptedException {
        client.create(pathname, value, OPEN_ACL_UNSAFE, mode);
        if (watch && client.exists(pathname, watch) == null) {
            throw KeeperException.create(NONODE, pathname);
        }
    }

    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return client.getChildren(path, watcher);
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        return client.getChildren(path, false);
    }

    public List<String> checkChildren(String path) throws KeeperException, InterruptedException {
        try {
            return client.getChildren(path, false);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public byte[] getData(String path) throws KeeperException, InterruptedException {
        return client.getData(path, null, null);
    }

    public byte[] getData(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return client.getData(path, watcher, null);
    }

    public byte[] checkData(String path, Watcher watcher) {
        try {
            return getData(path, watcher);
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    public void delete(String pathname) {
        System.out.println("[ZooKeeperUtil/delete] " + pathname);
        try {
            client.delete(pathname, -1);
        } catch (Exception e) {
            // ignore
        }
    }

    public String list() throws KeeperException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        list("", builder);
        return builder.toString();
    }

    private void list(String path, StringBuilder builder) throws KeeperException, InterruptedException {
        String target = path.isEmpty() ? "/" : path;
        builder.append(target);
        byte[] value = client.getData(target, false, null);
        if (value != null && value.length > 0) {
            builder.append('[');
            if (value.length < 20) {
                builder.append(new String(value));
            } else {
                builder.append(value.length);
            }
            builder.append(']');
        }
        builder.append('\n');
        for (String child : getChildren(target)) {
            list(path + "/" + child, builder);
        }
    }

    public void close() {
        try {
            client.close();
        } catch (Exception e) {
            // ignore
        }
    }
}
