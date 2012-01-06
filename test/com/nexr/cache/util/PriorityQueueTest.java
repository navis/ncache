package com.nexr.cache.util;

import org.junit.Test;

public class PriorityQueueTest {

    private static class Text implements Comparable<Text> {
        String string;

        public Text(String string) {
            this.string = string;
        }

        public int compareTo(Text o) {
            return string.compareTo(o.string);
        }

        public String toString() {
            return string;
        }
    }

    @Test
    public void testSorting() {
        PriorityQueue<Text> queue = testQueue();
        while (queue.size() > 0) {
            System.out.println(queue.pop());
        }
    }

    @Test
    public void testDownheap1() {
        PriorityQueue<Text> queue = testQueue();

        int index = 2;

        Text x = queue.get(index);
        System.out.println("down " + x + " to " + "123234");
        
        x.string = "123234";
        queue.downHeap(index);

        while (queue.size() > 0) {
            System.out.println(queue.pop());
        }
    }

    @Test
    public void testDownheap2() {
        PriorityQueue<Text> queue = testQueue();

        int index = 2;

        Text x = queue.get(index);
        System.out.println("down " + x + " to " + "zzzzz");

        x.string = "zzzzz";
        queue.downHeap(index);

        while (queue.size() > 0) {
            System.out.println(queue.pop());
        }
    }

    @Test
    public void testUpheap1() {
        PriorityQueue<Text> queue = testQueue();

        int index = 2;

        Text x = queue.get(index);
        System.out.println("up " + x + " to " + "123234");

        x.string = "123234";
        queue.upHeap(index);

        while (queue.size() > 0) {
            System.out.println(queue.pop());
        }
    }

    @Test
    public void testUpheap2() {
        PriorityQueue<Text> queue = testQueue();
        
        int index = 2;

        Text x = queue.get(index);
        System.out.println("up " + x + " to " + "zzzzz");

        x.string = "zzzzz";
        queue.upHeap(index);

        while (queue.size() > 0) {
            System.out.println(queue.pop());
        }
    }

    private PriorityQueue<Text> testQueue() {
        PriorityQueue<Text> queue = new InherentPriorityQueue<Text>(100, true);
        queue.put(new Text("navis"));
        queue.put(new Text("exarc"));
        queue.put(new Text("manse"));
        queue.put(new Text("misun"));
        queue.put(new Text("babo"));
        return queue;
    }
}
