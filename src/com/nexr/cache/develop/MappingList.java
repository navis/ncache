package com.nexr.cache.develop;

import java.util.*;

public class MappingList implements Iterable<IndexNode> {

    private transient IndexNode root;
    protected transient IndexNode header = new IndexNode(null, null);

    private transient int size;

    public MappingList() {
        header.next = header.previous = header;
    }

    public MappingList(int capacity) {
        header.next = header.previous = header;
        addFree(new int[]{0, capacity});
    }

    public synchronized int size() {
        return size;
    }

    public synchronized boolean addFree(int[] e) {
        IndexNode node = addBefore(header, e);
        putNode(node);
        return true;
    }

    public synchronized void clear() {
        IndexNode e = header.next;
        while (e != header) {
            IndexNode next = e.next;
            e.next = e.previous = null;
            e.element = null;
            e = next;
        }
        header.next = header.previous = header;
        root = null;
        size = 0;
    }

    public synchronized IndexNode addBefore(IndexNode entry, int[] e) {
        IndexNode newEntry = newNode(e, null);
        newEntry.addBefore(entry);
        return newEntry;
    }

    public synchronized IndexNode getEntry(int[] key) {
        // Offload comparator-based version for sake of performance
        IndexNode p = root;
        while (p != null) {
            int cmp = compare(key, p.element);
            if (cmp < 0) {
                p = p.left;
            } else if (cmp > 0) {
                p = p.right;
            } else {
                return p;
            }
        }
        return null;
    }

    public synchronized IndexNode reserve(int length) {
        System.out.println("[MappingList/reserve] " + length);
        IndexNode searched = searchFor(length);
        if (searched == null) {
            return searched;
        }
        int[] sliced = searched.slice(length);
        if (sliced == searched.element) {
            removeTree(searched);
            return searched;
        }

        adjustNode(searched);

        IndexNode index = addBefore(searched, sliced);
        index.state = IndexNode.IN_USE;

        System.out.println(this);
        return index;
    }


    public synchronized void release(IndexNode index) {
        System.out.println("[MappingList/release] " + index);
        IndexNode current = index;
        for (IndexNode prev; (prev = prevMerge(current)) != null; current = prev) {
            removeList(current);
        }
        for (IndexNode next; (next = nextMerge(current)) != null; current = next) {
            removeList(current);
        }
        current.state = IndexNode.FREE;
        System.out.println(this);

        putNode(current);
        System.out.println(this);
    }

    private IndexNode prevMerge(IndexNode current) {
        IndexNode prev = current.previous();
        if (prev != header && prev.prevMerge(current)) {
            removeTree(prev);
            return prev;
        }
        return null;
    }

    private IndexNode nextMerge(IndexNode current) {
        IndexNode next = current.next();
        if (next != header && next.nextMerge(current)) {
            removeTree(next);
            return next;
        }
        return null;
    }


    private void adjustNode(IndexNode searched) {
        IndexNode pred = predecessor(searched);
        if (pred != null) {
            removeTree(searched);
            putNode(searched);
        }
//        for (; pred != null && compare(searched.element, pred.element) < 0; pred = predecessor(pred)) {}
//        if (pred == null) {
//            searched.color = RED;
//            getFirstEntry().left = searched;
//            fixAfterInsertion(searched);
//        } else {
//
//        }
    }

    private IndexNode searchFor(int length) {
//        System.out.println("[TreeMap/ceilingEntry] " + Thread.currentThread() + " " + this);
        IndexNode p = root;
        while (p != null) {
            int cmp = search(length, p.element);
            if (cmp < 0) {
                if (p.left != null) {
                    p = p.left;
                } else {
                    return p;
                }
            } else if (cmp > 0) {
                if (p.right != null) {
                    p = p.right;
                } else {
                    IndexNode parent = p.parent;
                    IndexNode ch = p;
                    while (parent != null && ch == parent.right) {
                        ch = parent;
                        parent = parent.parent;
                    }
                    return parent;
                }
            } else {
                return p;
            }
        }
        return null;
    }

    public synchronized IndexNode putNode(IndexNode node) {
        System.out.println("---- [MappingList/putNode]    " + node);
        IndexNode t = root;
        if (t == null) {
            size = 1;
            return root = node;
        }
        int cmp;
        IndexNode parent;
        // split comparator and comparable paths
        do {
            parent = t;
            cmp = compare(node.element, t.element);
            if (cmp < 0) {
                t = t.left;
            } else if (cmp > 0) {
                t = t.right;
            } else {
                t.element = node.element;
                return t;
            }
        } while (t != null);

        node.parent = parent;
        if (cmp < 0) {
            parent.left = node;
        } else {
            parent.right = node;
        }
        fixAfterInsertion(node);
        size++;
        return null;
    }

    public synchronized void removeTree(IndexNode node) {
        System.out.println("---- [MappingList/removeTree] " + node);
        size--;
        // If strictly internal, copy successor's element to p and then make p
        // point to successor.
        if (node.left != null && node.right != null) {
            IndexNode s = successor(node);
            node.swapList(s);
            node = s;
        } // p has 2 children

        // Start fixup at replacement node, if it exists.
        IndexNode replacement = node.left != null ? node.left : node.right;

        if (replacement != null) {
            // Link replacement to parent
            replacement.parent = node.parent;
            if (node.parent == null) {
                root = replacement;
            } else if (node == node.parent.left) {
                node.parent.left = replacement;
            } else {
                node.parent.right = replacement;
            }
            // Null out links so they are OK to use by fixAfterDeletion.
            node.left = node.right = node.parent = null;
            // Fix replacement
            if (node.color == BLACK) {
                fixAfterDeletion(replacement);
            }
        } else if (node.parent == null) {
            // return if we are the only node.
            root = null;
        } else {
            //  No children. Use self as phantom replacement and unlink.
            if (node.color == BLACK) {
                fixAfterDeletion(node);
            }
            if (node.parent != null) {
                if (node == node.parent.left) {
                    node.parent.left = null;
                } else if (node == node.parent.right) {
                    node.parent.right = null;
                }
                node.parent = null;
            }
        }
        System.out.println(this);
    }

    public void removeList(IndexNode node) {
        System.out.println("---- [MappingList/removeList] " + node);
        node.previous.next = node.next;
        node.next.previous = node.previous;
        node.next = node.previous = null;
        node.element = null;
        size--;
    }

    protected IndexNode newNode(int[] value, IndexNode parent) {
        return new IndexNode(value, parent);
    }

    protected int compare(int[] index1, int[] index2) {
        if (index1[1] != index2[1]) {
            return index1[1] - index2[1];
        }
        return index1[0] - index2[0];
    }

    protected int search(int length, int[] index1) {
        if (length != index1[1]) {
            return length - index1[1];
        }
        return 0;
    }

    public Iterator<IndexNode> iterator() {
        return new EntryIterator(getFirstEntry());
    }

    abstract class PrivateEntryIterator implements Iterator<IndexNode> {

        IndexNode next;
        IndexNode lastReturned;

        PrivateEntryIterator(IndexNode first) {
            next = first;
        }

        public final boolean hasNext() {
            return next != null;
        }

        final IndexNode nextEntry() {
            IndexNode e = next;
            if (e == null) {
                throw new NoSuchElementException();
            }
            next = successor(e);
            lastReturned = e;
            return e;
        }

        final IndexNode prevEntry() {
            IndexNode e = next;
            if (e == null) {
                throw new NoSuchElementException();
            }
            next = predecessor(e);
            lastReturned = e;
            return e;
        }

        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    final class EntryIterator extends PrivateEntryIterator {

        EntryIterator(IndexNode first) {
            super(first);
        }

        public IndexNode next() {
            return nextEntry();
        }
    }

    private static final boolean RED = false;
    private static final boolean BLACK = true;

    /**
     * Returns the first Entry in the TreeMap (according to the TreeMap's
     * key-sort function).  Returns null if the TreeMap is empty.
     */
    public final IndexNode getFirstEntry() {
        IndexNode p = root;
        if (p != null) {
            while (p.left != null) {
                p = p.left;
            }
        }
        return p;
    }

    /**
     * Returns the last Entry in the TreeMap (according to the TreeMap's
     * key-sort function).  Returns null if the TreeMap is empty.
     */
    final IndexNode getLastEntry() {
        IndexNode p = root;
        if (p != null) {
            while (p.right != null) {
                p = p.right;
            }
        }
        return p;
    }

    /**
     * Returns the successor of the specified Entry, or null if no such.
     */
    public IndexNode successor(IndexNode t) {
        if (t == null) {
            return null;
        }
        if (t.right != null) {
            IndexNode p = t.right;
            while (p.left != null) {
                p = p.left;
            }
            return p;
        } else {
            IndexNode p = t.parent;
            IndexNode ch = t;
            while (p != null && ch == p.right) {
                ch = p;
                p = p.parent;
            }
            return p;
        }
    }

    /**
     * Returns the predecessor of the specified Entry, or null if no such.
     */
    public IndexNode predecessor(IndexNode t) {
        if (t == null) {
            return null;
        }
        if (t.left != null) {
            IndexNode p = t.left;
            while (p.right != null) {
                p = p.right;
            }
            return p;
        } else {
            IndexNode p = t.parent;
            IndexNode ch = t;
            while (p != null && ch == p.left) {
                ch = p;
                p = p.parent;
            }
            return p;
        }
    }

    /**
     * Balancing operations.
     * <p/>
     * Implementations of rebalancings during insertion and deletion are
     * slightly different than the CLR version.  Rather than using dummy
     * nilnodes, we use a set of accessors that deal properly with null.  They
     * are used to avoid messiness surrounding nullness checks in the main
     * algorithms.
     */

    private boolean colorOf(IndexNode p) {
        return p == null ? BLACK : p.color;
    }

    private IndexNode parentOf(IndexNode p) {
        return p == null ? null : p.parent;
    }

    private void setColor(IndexNode p, boolean c) {
        if (p != null) {
            p.color = c;
        }
    }

    private IndexNode leftOf(IndexNode p) {
        return p == null ? null : p.left;
    }

    private IndexNode rightOf(IndexNode p) {
        return p == null ? null : p.right;
    }

    /**
     * From CLR
     */
    private void rotateLeft(IndexNode p) {
        if (p != null) {
            IndexNode r = p.right;
            p.right = r.left;
            if (r.left != null) {
                r.left.parent = p;
            }
            r.parent = p.parent;
            if (p.parent == null) {
                root = r;
            } else if (p.parent.left == p) {
                p.parent.left = r;
            } else {
                p.parent.right = r;
            }
            r.left = p;
            p.parent = r;
        }
    }

    /**
     * From CLR
     */
    private void rotateRight(IndexNode p) {
        if (p != null) {
            IndexNode l = p.left;
            p.left = l.right;
            if (l.right != null) {
                l.right.parent = p;
            }
            l.parent = p.parent;
            if (p.parent == null) {
                root = l;
            } else if (p.parent.right == p) {
                p.parent.right = l;
            } else {
                p.parent.left = l;
            }
            l.right = p;
            p.parent = l;
        }
    }

    /**
     * From CLR
     */
    private void fixAfterDeletion(IndexNode x) {
        while (x != root && colorOf(x) == BLACK) {
            if (x == leftOf(parentOf(x))) {
                IndexNode sib = rightOf(parentOf(x));

                if (colorOf(sib) == RED) {
                    setColor(sib, BLACK);
                    setColor(parentOf(x), RED);
                    rotateLeft(parentOf(x));
                    sib = rightOf(parentOf(x));
                }

                if (colorOf(leftOf(sib)) == BLACK &&
                        colorOf(rightOf(sib)) == BLACK) {
                    setColor(sib, RED);
                    x = parentOf(x);
                } else {
                    if (colorOf(rightOf(sib)) == BLACK) {
                        setColor(leftOf(sib), BLACK);
                        setColor(sib, RED);
                        rotateRight(sib);
                        sib = rightOf(parentOf(x));
                    }
                    setColor(sib, colorOf(parentOf(x)));
                    setColor(parentOf(x), BLACK);
                    setColor(rightOf(sib), BLACK);
                    rotateLeft(parentOf(x));
                    x = root;
                }
            } else { // symmetric
                IndexNode sib = leftOf(parentOf(x));

                if (colorOf(sib) == RED) {
                    setColor(sib, BLACK);
                    setColor(parentOf(x), RED);
                    rotateRight(parentOf(x));
                    sib = leftOf(parentOf(x));
                }

                if (colorOf(rightOf(sib)) == BLACK &&
                        colorOf(leftOf(sib)) == BLACK) {
                    setColor(sib, RED);
                    x = parentOf(x);
                } else {
                    if (colorOf(leftOf(sib)) == BLACK) {
                        setColor(rightOf(sib), BLACK);
                        setColor(sib, RED);
                        rotateLeft(sib);
                        sib = leftOf(parentOf(x));
                    }
                    setColor(sib, colorOf(parentOf(x)));
                    setColor(parentOf(x), BLACK);
                    setColor(leftOf(sib), BLACK);
                    rotateRight(parentOf(x));
                    x = root;
                }
            }
        }

        setColor(x, BLACK);
    }

    /**
     * From CLR
     */
    private void fixAfterInsertion(IndexNode x) {
        x.color = RED;

        while (x != null && x != root && x.parent.color == RED) {
            if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
                IndexNode y = rightOf(parentOf(parentOf(x)));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                } else {
                    if (x == rightOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateLeft(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    rotateRight(parentOf(parentOf(x)));
                }
            } else {
                IndexNode y = leftOf(parentOf(parentOf(x)));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                } else {
                    if (x == leftOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateRight(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    rotateLeft(parentOf(parentOf(x)));
                }
            }
        }
        root.color = BLACK;
    }

    public void dump() {
        if (root == null) {
            System.out.println("null");
            return;
        }
        String space = "   ";
        List<IndexNode> next = next(root, new ArrayList<IndexNode>(), space);

        for (; !next.isEmpty();) {
            space += "  ";
            List<IndexNode> child = new ArrayList<IndexNode>();
            for (IndexNode node : next) {
                next(node, child, space);
            }
            next = child;
        }
    }

    private List<IndexNode> next(IndexNode node, List<IndexNode> next, String space) {
        System.out.println(space + dumpNode(node));
        if (node.left != null) {
            next.add(node.left);
        }
        if (node.right != null) {
            next.add(node.right);
        }
        return next;
    }

    public String dumpNode(IndexNode node) {
//        return node.value + " [" + node.hashCode() + "]p:" + node.parent + ", l:" + node.left + ", r:"  + node.right + ", c:" + (node.color ? "B" : "R");
        return node + ",l=" + node.left + ",r=" + node.right;
    }

    public synchronized String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("F: ");
        for (IndexNode entry : this) {
            builder.append(entry).append("--");
        }
        builder.append("\nL: ");

        for (IndexNode current = header.next; current != header; current = current.next) {
            builder.append(current).append("--");
        }
        return builder.toString();
    }
}