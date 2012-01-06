package com.nexr.cache.cluster;

import java.util.Arrays;

import org.junit.Test;

import com.nexr.cache.PartitionUtil;

public class PartitionUtilTest {
    
    @Test
    public void newPartition() {
        for (int[] partition : PartitionUtil.newPartition(10, 5, 2)) {
            System.out.println(Arrays.toString(partition));
        }
        for (int[] partition : PartitionUtil.newPartition(10, 3, 6)) {
            System.out.println(Arrays.toString(partition));
        }
    }
}
