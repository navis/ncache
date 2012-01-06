package com.nexr.cache.develop;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NavisConnectionFactory;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.vbucket.config.Config;

public class SimpleUpdatableLocator implements UpdatableLocator {

    List<MemcachedNode> nodes;
    NavisConnectionFactory factory;

    public SimpleUpdatableLocator(NavisConnectionFactory factory, List<MemcachedNode> nodes) {
        this.factory = factory;
    }

    public void addNode(SocketAddress address) {
        DefaultConnectionFactory factory = new DefaultConnectionFactory();
    }

    public void update(String name, GroupState state) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public MemcachedNode getPrimary(String k) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Iterator<MemcachedNode> getSequence(String k) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Collection<MemcachedNode> getAll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NodeLocator getReadonlyCopy() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void updateLocator(List<MemcachedNode> memcachedNodes, Config config) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
