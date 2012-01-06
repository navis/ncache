package com.nexr.cache.develop;

import net.spy.memcached.AddrUtil;
import net.rubyeye.memcached.benchmark.StringGenerator;
import com.nexr.cache.develop.SimpleMClient;

public class MemcachedClientTest {

    public static void main(String[] args) throws Exception {
//        MemcachedClient memcachedClient = new MemcachedClient(AddrUtil.getAddresses(args[0]));
        SimpleMClient memcachedClient = new SimpleMClient(AddrUtil.getAddresses(args[0]));

        long prev = System.currentTimeMillis();

        for (int i = 1; i < 1024 << 6; i++) {
//            memcachedClient.set("navis" + i, 0, StringGenerator.generate(i, 64)).get();
            memcachedClient.set("navis" + i, 0, 0, StringGenerator.generate(i, 64));
        }
        for (int i = 1; i < 1024 << 6; i++) {
            final Object value = memcachedClient.get("navis" + i);
            if (!value.equals(StringGenerator.generate(i, 64))) {
                System.out.println("-- [MemcachedClientTest/main] '" + value + "' expected '" + StringGenerator.generate(i, 64) + "'");
            }
        }
        System.out.println("[MemcachedClientTest/main] " + (System.currentTimeMillis() - prev) + " msec");
        memcachedClient.shutdown();
    }
}
