package com.nexr.cache.cluster;

import java.util.Arrays;

import org.junit.Test;

import com.nexr.cache.*;
import com.nexr.cache.util.Property;
import com.nexr.cache.util.Generations;
import com.nexr.cache.message.Snapshot;


public class NameSpaceManagerTest {

//    @Test
//    public void test() throws Exception {
//        final NameSpaceManager manager = new NameSpaceManager();
//        final ServerResource resource = new CacheServerResource();
//        resource.initialize(new Property(null));
//
//        String expression = "test:server0";
//        ServerConnectionManager cluster = new ServerConnectionManager(new FullName(expression), resource);
//        resource.intialized(cluster, null);
//
//        int[][] partition = PartitionUtil.newPartition(271, 3, 3);
//        cluster.initEntity(Arrays.asList("0:server0", "1:server1", "2:server2"));
//
//        NameSpaceManager<NameSpace> nsmanager = new NameSpaceManager<NameSpace>();
//        nsmanager.namespaceFor(0).syncPartition(partition);
//
//        ServerName name0 = new ServerName(0, "server0");
//        ServerName name1 = new ServerName(1, "server1");
//        ServerName name2 = new ServerName(2, "server2");
//
//        ServerEntity entity0 = new ServerEntity(name0);
//        ServerEntity entity1 = new ServerEntity(name1);
//        ServerEntity entity2 = new ServerEntity(name2);
//
//        final long[][][] report0 = new long[][][]{new long[][]{generations(0, 100, 0, 0), generations(0, 101, 0, 0), generations(0, 50, 0, 0)}};
//        final long[][][] report1 = new long[][][]{new long[][]{generations(1, 100, 0, 0), generations(1, 100, 81, 0), generations(1, 50, 30, 20)}};
//        final long[][][] report2 = new long[][][]{new long[][]{generations(2, 100, 0, 30), generations(2, 100, 80, 10), generations(2, 40, 30, 30)}};
//
//        Snapshot snapshot1 = new Snapshot(report1);
//        snapshot1.source(entity1);
//        Snapshot snapshot2 = new Snapshot(report2);
//        snapshot2.source(entity2);
//
//        for (long[] recovered : report0[0]) {
//            toString(recovered);
//        }
//
//        System.out.println("[NameSpaceManagerTest/test] ");
//
//        GenerationManager gmanager = new GenerationManager() {
//            public long generation(int namespace, int partition, int pindex) {
//                return report0[namespace][partition][pindex];
//            }
//            public long recoverable(int namespace, int partition, int pindex, long generation) {
//                return generation;
//            }
//        };
//        NameSpaceManager.RecoveryListener listener = manager.recoveryListener(gmanager, cluster, 1);
//        listener.arrived(snapshot1);
//        listener.arrived(snapshot2);
//
//        listener.requestSync(Arrays.asList(entity1, entity2));
//
//        for (long[] recovered : report0[0]) {
//            toString(recovered);
//        }
//    }
//
//    private long[] generations(int sindex, long... generation) {
//        long[] generations = new long[generation.length];
//        for (int i = 0; i < generations.length; i++) {
//            generations[i] = Generations.MARK(sindex, generation[i]);
//        }
//        return generations;
//    }
//
//    private void toString(long[] recovered) {
//        StringBuilder builder = new StringBuilder();
//        for (int i = 0; i < recovered.length; i++) {
//            builder.append(Generations.SERVER(recovered[i])).append(':').append(Generations.COUNTER(recovered[i]));
//            if (i < recovered.length - 1) {
//                builder.append(", ");
//            }
//        }
//        System.out.println(builder.toString());
//    }
}
