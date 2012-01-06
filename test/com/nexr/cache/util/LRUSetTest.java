package com.nexr.cache.util;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import junit.framework.Assert;

public class LRUSetTest {

    @Test
    public void testCLRU() {
        LinkedSet<String> list = new LinkedSet<String>(true);
        list.put("navis1");
        list.put("navis2");
        list.put("navis3");
        list.put("navis4");
        String[] array1 = list.values().toArray(new String[0]);
        String[] array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis1", "navis2", "navis3", "navis4"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis1", "navis2", "navis3", "navis4"}));

        list.get("navis1");
        array1 = list.values().toArray(new String[0]);
        array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis2", "navis3", "navis4", "navis1"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis1", "navis2", "navis3", "navis4"}));

        list.remove("navis3");
        array1 = list.values().toArray(new String[0]);
        array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis2", "navis4", "navis1"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis1", "navis2", "navis4"}));
    }

    @Test
    public void testCLRU2() {
        LRUSet<String> list = new LRUSet<String>(20) {
            protected int toLength(String s) {
                return s.length();
            }

            protected String toString(String s) {
                return s;
            }
        };

        list.put("navis1");
        list.put("navis2");
        list.put("navis3");
        list.put("navis4");
        list.put("navis5");
        list.put("navis6");

        String[] array1 = list.values().toArray(new String[0]);
        String[] array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis3", "navis4", "navis5", "navis6"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis3", "navis4", "navis5", "navis6"}));

        list.get("navis6");
        array1 = list.values().toArray(new String[0]);
        array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis3", "navis4", "navis5", "navis6"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis3", "navis4", "navis5", "navis6"}));

        list.get("navis3");
        array1 = list.values().toArray(new String[0]);
        array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis4", "navis5", "navis6", "navis3"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis3", "navis4", "navis5", "navis6"}));

        list.remove("navis6");
        array1 = list.values().toArray(new String[0]);
        array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis4", "navis5", "navis3"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis3", "navis4", "navis5"}));

        list.put("navis7");
        list.put("navis8");
        array1 = list.values().toArray(new String[0]);
        array2 = list.cvalues().toArray(new String[0]);
        Assert.assertTrue(Arrays.equals(array1, new String[] {"navis5", "navis3", "navis7", "navis8"}));
        Assert.assertTrue(Arrays.equals(array2, new String[] {"navis3", "navis5", "navis7", "navis8"}));
    }

    @Test
    public void testCursor() {

        LRUSet<String> map = new LRUSet<String>(1000) {
            protected int toLength(String s) {
                return s.length();
            }

            protected String toString(String s) {
                return s;
            }
        };
        for (int i = 0; i < 100; i++) {
            map.put("navis" + i);
        }
        String[] iterated = toList(map.cursor(), 3);
        Assert.assertTrue(Arrays.equals(iterated, new String[] {"navis0", "navis1", "navis2"}));

        iterated = toList(map.cursor(), 3);
        Assert.assertTrue(Arrays.equals(iterated, new String[] {"navis3", "navis4", "navis5"}));

        map.remove("navis6");
        map.remove("navis7");
        map.remove("navis10");
        iterated = toList(map.cursor(), 3);
        Assert.assertTrue(Arrays.equals(iterated, new String[] {"navis8", "navis9", "navis11"}));
    }

    private String[] toList(Iterator<String> iterator, int size) {
        String[] array = new String[size];
        int i = 0;
        for (; i < size && iterator.hasNext(); i++) {
            array[i] = iterator.next();
        }
        return i < size ? Arrays.copyOfRange(array, 0, size) : array;
    }

    private char[] nextKey(char[] start) {
        int i = start.length - 1;
        for (; i >= 0 ; i--) {
            if (start[i] != 'z') {
                break;
            }
        }
        char[] next = new char[start.length];
        if (i > 0) {
            System.arraycopy(start, 0, next, 0, i);
        }
        next[i] = (char) (start[i] + 1);
        for (i++; i < start.length; i++) {
            next[i] = 'a';
        }
        return next;
    }
}
