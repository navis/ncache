package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.List;
import java.io.IOException;

import com.nexr.cache.develop.UpdatableLocator;
import com.nexr.cache.develop.SimpleUpdatableLocator;

public class NavisConnectionFactory extends DefaultConnectionFactory {

    MemcachedConnection connection;

    @Override
    public MemcachedConnection createConnection(List<InetSocketAddress> addrs)
		throws IOException {
        connection.getLocator();
        return connection = super.createConnection(addrs);
	}

    @Override
    public NodeLocator createLocator(List<MemcachedNode> nodes) {
		return new SimpleUpdatableLocator(this, nodes);
	}

    public UpdatableLocator locator() {
        return (UpdatableLocator) connection.getLocator();
    }
}
