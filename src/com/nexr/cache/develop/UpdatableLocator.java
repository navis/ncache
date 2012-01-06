package com.nexr.cache.develop;

import java.net.SocketAddress;

import net.spy.memcached.NodeLocator;

public interface UpdatableLocator extends NodeLocator {

    void addNode(SocketAddress address);

    void update(String name, GroupState state);
}
