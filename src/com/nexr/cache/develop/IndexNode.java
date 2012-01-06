package com.nexr.cache.develop;

public class IndexNode {

    private static final boolean RED = false;
    private static final boolean BLACK = true;

    public static final int FREE = 0;
    public static final int IN_USE = 1;
    public static final int DISCARD = 2;

    IndexNode left;
    IndexNode right;
    IndexNode parent;
    boolean color = BLACK;

    int state;
    int[] element;
    IndexNode next;
    IndexNode previous;

    protected void addBefore(IndexNode entry) {
        next = entry;
        previous = entry.previous;
        previous.next = this;
        entry.previous = this;
    }

    IndexNode(int[] element, IndexNode parent) {
        this.element = element;
        this.parent = parent;
    }

    synchronized int[] slice(int length) {
        if (element[1] == length) {
            state = IN_USE;
            return element;
        }
        int[] sliced = new int[]{element[0], length};
        element[0] += length;
        element[1] -= length;

        return sliced;
    }

    synchronized boolean prevMerge(IndexNode current) {
        if (state == FREE) {
            System.out.println("-- [IndexNode/prevMerge] " + this + "+" + current);
            element[1] += current.element[1];
            return true;
        }
        return false;
    }

    synchronized boolean nextMerge(IndexNode current) {
        if (state == FREE) {
            System.out.println("-- [IndexNode/nextMerge] " + current + "+" + this);
            element[0] -= current.element[1];
            element[1] += current.element[1];
            return true;
        }
        return false;
    }

    synchronized void discard() {
        previous = next = null;
        state = DISCARD;
    }

    synchronized IndexNode previous() {
        return previous;
    }

    synchronized IndexNode next() {
        return next;
    }

    public String toString() {
        return state + "[" + element[0] + ":" + element[1] + "]" + "[" + hashCode() + "]";
    }

    public synchronized void swapList(IndexNode source) {
        
        int state = this.state;
        int[] element = this.element;
        IndexNode next = this.next;
        IndexNode previous = this.previous;

        this.state = source.state;
        this.element = source.element;
        this.next = source.next;
        this.previous = source.previous;

        source.state = state;
        source.element = element;
        source.next = next;
        source.previous = previous;
    }
}
