package com.nexr.cache.util;

import java.util.Random;

import org.junit.Test;

public class LRUMapTest {

    Random random = new Random();

    @Test
    public void testLRU() {

        int maximum = 2000;

        LRUMap<String, int[]> map = new LRUMap<String, int[]>(maximum) {

            protected int toLength(String key, int[] ints) {
                return ints[0];
            }

            protected String toString(String key, int[] ints) {
                return key + "=" + ints[0];
            }
        };

        for (int i =0; i < 1000; i++) {
            String key = randomKey(3);
            int length = random.nextInt(54) + 1;
            map.put(key, new int[] {length});
        }
    }

    private String randomKey(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char)('a' + random.nextInt(26)));
        }
        return builder.toString();
    }
}
