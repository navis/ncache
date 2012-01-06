package com.nexr.cache;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.nexr.cache.util.Generations;
import static com.nexr.cache.Record.*;
import static com.nexr.cache.Record.KLENGTH_OFFSET;

public class TestUtils {

    public static int[][] eindex(MemoryPool pool, String key, String value, int server, long counter) {
        return eindex(pool, key.getBytes(), value.getBytes(), server, counter);
    }

    public static int[][] eindex(MemoryPool pool, byte[] key, byte[] value, int server, long counter) {
        int hash = Utils.hash(key);
        long generation = Generations.NEW(server, counter);
        return new int[][] { Generations.INDEX(server, counter, hash), allocate(pool, key, hash, value, generation)};
    }

    public static int[] allocate(MemoryPool pool, byte[] key, int hash, byte[] value, long generation) {
        int[] index = pool.allocate(KEY_OFFSET + key.length + value.length);
        ByteBuffer buffer = pool.regionFor(index, true);
        buffer.putLong(GSTAMP_OFFSET, generation);
        buffer.putInt(HASH_OFFSET, hash);
        buffer.putInt(EXPIRE_OFFSET, -1);
        buffer.put(FLAGS_OFFSET, (byte)0);
        buffer.putShort(KLENGTH_OFFSET, (short) key.length);
        buffer.position(KEY_OFFSET);
        buffer.put(key);
        buffer.put(value);
        return index;
    }

    public static String[] toString(RecordManager record, Collection<int[][]> eindexes, boolean keyOnly) {
        int counter = 0;
        String[] values = new String[eindexes.size()];
        for (int[][] eindex : eindexes) {
            int[] mindex = eindex[Constants.MEMORY];
//            System.out.println("[TestUtils/toString] " + Region.toString(mindex));
            values[counter++] = keyOnly ? record.keyString(mindex) : record.keyValueString(mindex);
        }
        return values;
    }

    public static boolean equals(RecordManager record, Collection<int[][]> eindexes, boolean keyOnly, String... expected) {
        String[] result = toString(record, eindexes, keyOnly);
        for (int i = 0; i < Math.min(result.length, expected.length); i++) {
            if (!expected[i].equals(result[i])) {
                System.out.println("[TestUtils/equals] " + (i + 1)+ " th, expected = " + expected[i] + ", real = " + result[i]);
                return false;
            }
        }
        if (result.length > expected.length) {
            System.out.println("[TestUtils/equals] has more results.. " + result.length);
            return false;
        }
        if (result.length < expected.length) {
            System.out.println("[TestUtils/equals] has less results.. " + result.length);
            return false;
        }
        return true;
    }

    public static boolean equals(Collection<String> values, String... expected) {
        int index = 0;
        String[] array = new String[values.size()];
        for (String value : values) {
            array[index++ ] = value;
        }
        return equals(array, expected);
    }

    public static boolean equals(String[] values, String... expected) {
        for (int i = 0; i < Math.min(values.length, expected.length); i++) {
            if (!expected[i].equals(values[i])) {
                System.out.println("[TestUtils/equals] " + (i + 1)+ " th, expected = " + expected[i] + ", real = " + values[i]);
                return false;
            }
        }
        if (values.length > expected.length) {
            System.out.println("[TestUtils/equals] has more results.. " + values.length);
            return false;
        }
        if (values.length < expected.length) {
            System.out.println("[TestUtils/equals] has less results.. " + values.length);
            return false;
        }
        return true;
    }
}
