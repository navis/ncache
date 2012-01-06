package com.nexr.cache;

import static com.nexr.cache.Constants.MEMORY;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import static com.nexr.cache.Record.*;
import com.nexr.cache.util.Generations;
import junit.framework.Assert;

public class KVIndexCacheTest {

    Random random = new Random();

    @Test
    public void test1() {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(32, 0.25f, 4 << 10);

        RecordManager manager = new RecordManager(pool);
        KVIndexCache cache = new KVIndexCache(manager);

        String keystr = "navis1";
        String valurstr = "value1";
        byte[] key = keystr.getBytes();
        byte[] value = valurstr.getBytes();

        long counter = 0;
        int[][] eindex1 = TestUtils.eindex(pool, key, value, 0, ++counter);
        int[][] eindex2 = TestUtils.eindex(pool, "navis2", "value2", 0, ++counter);
        int[][] eindex3 = TestUtils.eindex(pool, "navis3", "value3", 0, ++counter);

        int length = Region.toLength(eindex1[MEMORY]);
        Assert.assertEquals(length, RECORD_HEADER_LEN + keystr.length() + valurstr.length());

        Assert.assertNull(cache.store(0, eindex1));
        
        int[][] retrieved = cache.get(key, Utils.hash(key));
        Assert.assertSame(eindex1[MEMORY], retrieved[MEMORY]);
        Assert.assertEquals(keystr, manager.keyString(eindex1[MEMORY]));
        Assert.assertEquals(valurstr, manager.valueString(eindex1[MEMORY]));
        Assert.assertEquals(keystr + "=" + valurstr, manager.keyValueString(eindex1[MEMORY], (byte) '='));

        int[] prev = eindex1[MEMORY];
        int[] nindex = eindex1[MEMORY] = manager.cropData(eindex1[MEMORY]);
        System.out.println(Region.toString(eindex1[MEMORY]) + " --> " + Region.toString(nindex));
        Assert.assertNotSame(prev, nindex);
        Assert.assertSame(eindex1, cache.get(key, Utils.hash(key)));
        Assert.assertEquals(keystr, manager.keyString(nindex));
        Assert.assertEquals(length - valurstr.length(), Region.toLength(nindex));

        cache.store(0, eindex2);
        cache.store(0, eindex3);

        Assert.assertTrue(TestUtils.equals(manager, cache.values(), true, "navis1", "navis2", "navis3"));

        Assert.assertEquals("value2", manager.valueString(cache.retrieve("navis2")));
        Assert.assertTrue(TestUtils.equals(manager, cache.values(), true, "navis1", "navis3", "navis2"));
        Assert.assertTrue(TestUtils.equals(manager, cache.cvalues(), true, "navis1", "navis2", "navis3"));

        System.out.println("LRU : " + Arrays.toString(TestUtils.toString(manager, cache.values(), false)));
        System.out.println("C   : " + Arrays.toString(TestUtils.toString(manager, cache.cvalues(), false)));

        System.out.println("[KVIndexCacheTest/test1] remove mark " + manager.keyString(eindex1[MEMORY]));
        cache.removeMark(eindex1);
        Assert.assertNull(cache.retrieve("navis1"));
        Assert.assertTrue(TestUtils.equals(manager, cache.values(), false, "navis1", "navis3:value3", "navis2:value2"));
        Assert.assertTrue(TestUtils.equals(manager, cache.cvalues(), false, "navis2:value2", "navis3:value3", "navis1"));

        System.out.println("LRU : " + Arrays.toString(TestUtils.toString(manager, cache.values(), false)));
        System.out.println("C   : " + Arrays.toString(TestUtils.toString(manager, cache.cvalues(), false)));
    }

    @Test
    public void test2() {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(32, 0.25f, 4 << 10);

        RecordManager manager = new RecordManager(pool);
        KVIndexCache cache = new KVIndexCache(manager);

        String[] keys = new String[]{
                "ubagenqfjvkc", "ktkpsswntwqt", "sfcxepjarbnu", "xrfqmhfzjiog", "sfcxepjarbnu",
                "wjjwyfqtwylt", "mzlhkxufdhhz", "lwrevhyanpuk", "rowbyxlspwjg", "jyqrzyqhwwdi",
                "xrfqmhfzjiog", "rlsviysnrvpq", "vwfhoityhqri", "pvgrcyexsuwk", "wngnwzmvbxir",
                "lehphblvlonq", "ulvlwgpsyfrr"};
        byte[] value = new byte[1024];

        long counter = 0;
        for (String key : keys) {
            byte[] bkey = key.getBytes();
            int hash = Utils.hash(bkey);
            int[][] eindex = TestUtils.eindex(pool, bkey, value, 0, ++counter);
            cache.store(0, eindex);
        }
        byte[] bkey = "ktkpsswntwqt".getBytes();
        int hash = Utils.hash(bkey);
        Assert.assertNotNull(cache.remove(bkey, hash));
    }

    @Test
    public void randomTest() {
        MemcachedMPool pool = new MemcachedMPool();
        pool.initialize(32, 0.25f, 4 << 10);

        RecordManager manager = new RecordManager(pool);
        KVIndexCache cache = new KVIndexCache(manager);

        for (int i = 0; i < 2000; i++) {
            String key = randomStr(2);
            int hash = Utils.hash(key.getBytes());
            String value = randomStr(4096);

            int[] index = pool.allocate(KLENGTH_OFFSET + KLENGTH_LENTH + key.length() + value.length());
            ByteBuffer buffer = pool.regionFor(index, true);
            buffer.position(KLENGTH_OFFSET);
            buffer.putShort((short) key.length());
            buffer.put(key.getBytes());
            buffer.put(value.getBytes());

            int[] information = Generations.INDEX(0l, hash);
            int[][] eindex = new int[][]{information, index};
            cache.store(0, eindex);
        }
    }

    private String randomStr(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return builder.toString();
    }
}
