package com.nexr.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Map;
import java.util.HashMap;

public class PartitionUtil {

    public static byte[] addressToBytes(String address) throws IOException {
        ByteArrayOutputStream boutput = new ByteArrayOutputStream();
        new DataOutputStream(boutput).writeUTF(address);
        return boutput.toByteArray();
    }

    public static int[][] newPartition(int partitions, int replications, int servers) {
        replications = Math.min(replications, servers);
        int[][] partition = new int[partitions][replications];
        for (int i = 0; i < partition.length; i++) {
            for (int j = 0; j < partition[i].length; j++) {
                partition[i][j] = (i + j) % servers;
            }
        }
        return partition;
    }

    public static int[][] rePartition(int[][] partition, long[][] usages, int increase) {
        return partition;
    }

    public static byte[] partitionToBytes(int[][] partition) throws IOException {
        ByteArrayOutputStream boutput = new ByteArrayOutputStream();
        PartitionUtil.writePartition(new DataOutputStream(boutput), partition);
        return boutput.toByteArray();
    }

    public static void writePartition(DataOutput output, int[][] partition) throws IOException {
        output.writeInt(partition.length);
        output.writeInt(partition[0].length);
        for (int[] servers : partition) {
            for (int server : servers) {
                output.writeInt(server);
            }
        }
    }

    public static int[][] readPartition(byte[] input) throws IOException {
        return readPartition(new DataInputStream(new ByteArrayInputStream(input)));
    }

    public static int[][] readPartition(DataInput input) throws IOException {
        int[][] partition = new int[input.readInt()][input.readInt()];
        for (int i = 0; i < partition.length; i++) {
            for (int j = 0; j < partition[i].length; j++) {
                partition[i][j] = input.readInt();
            }
        }
        return partition;
    }

    public static IndexedServerName[] readServers(byte[] input) throws IOException {
        return readServers(new DataInputStream(new ByteArrayInputStream(input)));
    }

    public static IndexedServerName[] readServers(DataInput input) throws IOException {
        IndexedServerName[] servers = new IndexedServerName[input.readInt()];
        for (int i = 0; i < servers.length; i++) {
            servers[i] = new IndexedServerName(input);
        }
        return servers;
    }

    public static IndexedServerName parseServerName(String expressions) {
        int index0 = expressions.indexOf(':');
        int index = Integer.valueOf(expressions.substring(0, index0));
        return new IndexedServerName(index, expressions.substring(index0 + 1));
    }

    public static Map<String, String> parseAddress(byte[] expressions) {
        return parseAddress(new String(expressions));
    }

    public static Map<String, String> parseAddress(String expressions) {
        Map<String, String> addresses = new HashMap<String, String>();
        for (String expression : expressions.split(",")) {
            int delim = expression.indexOf('=');
            String protocol = delim < 0 ? Constants.NCACHE_ADDRESS : expression.substring(0, delim);
            String address = delim < 0 ? expression : expression.substring(delim + 1);
            addresses.put(protocol, address);
        }
        System.out.println("[PartitionUtil/parseAddress] " + addresses);
        return addresses;
    }

    public static long[] readUsage(byte[] input) throws IOException {
        return readUsage(new DataInputStream(new ByteArrayInputStream(input)));
    }

    public static long[] readUsage(DataInput input) throws IOException {
        long[] usage = new long[input.readInt()];
        for (int i = 0; i < usage.length; i++) {
            usage[i] = input.readLong();
        }
        return usage;
    }
}
