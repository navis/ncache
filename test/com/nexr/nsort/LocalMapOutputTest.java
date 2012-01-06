package com.nexr.nsort;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import org.junit.Test;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.IndexedSorter;

import junit.framework.Assert;

public class LocalMapOutputTest {

    @Test
    public void testReader() {
        byte[] data = "navis\t\tmanse\n".getBytes();
        LocalMapOutput.LineReader reader = new LocalMapOutput.LineReader(data, 2);
        int[][] index = reader.nextLine(0);
        Assert.assertNotNull(index);
        Assert.assertEquals("navis", new String(data, index[1][0], index[1][1]));
        Assert.assertEquals("manse", new String(data, index[2][0], index[2][1]));

        Assert.assertNull(reader.nextLine(1));
    }

    @Test
    public void testReader2() {
        byte[] data = "010010 99999  19900101    32.1  7    31.9  7  1016.2  7  9999.9  0    1.6  7    1.0  7    2.9    4.1    33.3    30.2   0.21F 999.9  101000\n010010 99999  19900102    32.7  6    32.5  6  1012.1  6  9999.9  0    3.8  6    6.0  6   15.9   22.0    36.0    31.6*  0.12E 999.9  110000".getBytes();
        LocalMapOutput.LineReader reader = new LocalMapOutput.LineReader(data, 22, (byte) ' ');
        int[][] index = reader.nextLine(0);
        Assert.assertEquals("010010", new String(data, index[1][0], index[1][1]));
        Assert.assertEquals("99999", new String(data, index[2][0], index[2][1]));
        Assert.assertEquals("19900101", new String(data, index[3][0], index[3][1]));
        Assert.assertEquals("101000", new String(data, index[22][0], index[22][1]));

        index = reader.nextLine(1);
        Assert.assertEquals("010010", new String(data, index[1][0], index[1][1]));
        Assert.assertEquals("99999", new String(data, index[2][0], index[2][1]));
        Assert.assertEquals("19900102", new String(data, index[3][0], index[3][1]));
        Assert.assertEquals("110000", new String(data, index[22][0], index[22][1]));
    }

    public static void main(String[] args) throws Exception {
        File file = new File("C:\\Documents and Settings\\navis\\My Documents\\Downloads\\gsod_2009\\gsod_1990.op");
        byte[] data = new byte[128<<20];
        FileInputStream input = new FileInputStream(file);
        input.read(data);

//        byte[] data = (
//                "c1_10  c2_20  c3_30  c4_40\n" +
//                "c1_20  c2_40  c3_10  c4_30\n" +
//                "c1_40  c2_30  c3_20  c4_10\n" +
//                "c1_30  c2_10  c3_40  c4_20\n").getBytes();

        IndexedSorter sort = new QuickSort();
        long prev = System.currentTimeMillis();
        LocalMapOutput output = new LocalMapOutput(data, 22, (byte) ' ');
        System.out.println(" indexing..  " + (System.currentTimeMillis() - prev) + " msec");
        for (int i = 1; i <= 22; i++) {
//        for (int i : new int[]{1, 2, 3, 4}) {
            prev = System.currentTimeMillis();
            output.sorting = i;
            sort.sort(output, 0, output.index.length);
            System.out.println(" sorting " + i + "..  " + (System.currentTimeMillis() - prev) + " msec");
        }

//        int[] shuffle = output.shuffle(1, 2);
//        System.out.println("[LocalMapOutputTest/main] " + Arrays.toString(shuffle));

//        System.out.println("[LocalMapOutputTest/main] 1");
//        Iterator<byte[]> iterator = output.iterate(1);
//        int counter = 0;
//         while (iterator.hasNext()) {
//            byte[] value = iterator.next();
//            if ((counter + 1) % 15 == 0) {
//                System.out.println(new String(value));
//            }
//            counter++;
//        }

//        System.out.println("[LocalMapOutputTest/main] 2");
//        iterator = output.iterate(2);
//        counter = 0;
//         while (iterator.hasNext()) {
//            byte[] value = iterator.next();
//            if ((counter + 1) % 15 == 0) {
//                System.out.println(new String(value));
//            }
//            counter++;
//        }

//        System.out.println("[LocalMapOutputTest/main] 1 to 2");
//        iterator = output.iterate2(1, 2, shuffle);
//        counter = 0;
//         while (iterator.hasNext()) {
//            byte[] value = iterator.next();
//            if ((counter + 1) % 15 == 0) {
//                System.out.println(new String(value));
//            }
//            counter++;
//        }

//
        output.saveAll(new File("./ix2"));

        for (int i = 1; i <= 22; i++) {
            prev = System.currentTimeMillis();
            Iterator<byte[]> iterator = output.iterate(i);
            while (iterator.hasNext()) {
                iterator.next();
            }
            System.out.println(" iterating " + i + "..  " + (System.currentTimeMillis() - prev) + " msec");
        }

//        int[] shuffle = new int[958697];
//        for (int i = 0; i < shuffle.length; i++) {
//            shuffle[i] = shuffle.length - i - 1;
//            shuffle[i] = i;
//        }
//        shuffle(shuffle, new Random());
//
        int accum = 0;
        int counter = 0;
        for (int i = 1; i <= 22; i++) {
            prev = System.currentTimeMillis();
            Iterator<byte[]> iterator = LocalMapOutput.iterate(new File("./ix2"), i);
            while (iterator.hasNext()) {
                byte[] value = iterator.next();
                if ((counter + 1) % 500000 == 0) {
                    System.out.println(new String(value));
                }
                accum += value.length;
                counter++;
            }
            System.out.println(" iterating " + i + "..  " + (System.currentTimeMillis() - prev) + " msec, " + accum + " bytes");
            accum = 0;
        }
    }

    public static void shuffle(int[] list, Random rnd) {
        int size = list.length;
        for (int i = size; i > 1; i--) {
            swap(list, i - 1, rnd.nextInt(i));
        }
    }

    public static void swap(int[] list, int i, int j) {
        int a = list[i];
        list[i] = list[j];
        list[j] = a;
    }
}
