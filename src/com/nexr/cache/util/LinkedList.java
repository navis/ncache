package com.nexr.cache.util;

import java.util.*;

public class LinkedList<E> {

    protected transient Entry<E> header = new Entry<E>(null);
    protected transient int size;

    public LinkedList() {
        header.next = header.previous = header;
    }

    public E getFirst() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        return header.next.element;
    }

    public E getLast() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        return header.previous.element;
    }

    public E removeFirst() {
        return remove(header.next);
    }

    public E removeLast() {
        return remove(header.previous);
    }

    public Entry<E> addFirst(E e) {
        return addBefore(header.next, e);
    }

    public Entry<E> addLast(E e) {
        return addBefore(header, e);
    }

    public int size() {
        return size;
    }

    public boolean add(E e) {
        addBefore(header, e);
        return true;
    }

    public void clear() {
        Entry<E> e = header.next;
        while (e != header) {
            Entry<E> next = e.next;
            e.next = e.previous = null;
            e.element = null;
            e = next;
        }
        header.next = header.previous = header;
        size = 0;
    }

    public E get(int index) {
        return entry(index).element;
    }

    public E set(int index, E element) {
        Entry<E> e = entry(index);
        E oldVal = e.element;
        e.element = element;
        return oldVal;
    }

    public void add(int index, E element) {
        addBefore((index == size ? header : entry(index)), element);
    }

    public E remove(int index) {
        return remove(entry(index));
    }

    private Entry<E> entry(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        Entry<E> e = header;
        if (index < size >> 1) {
            for (int i = 0; i <= index; i++) {
                e = e.next;
            }
        } else {
            for (int i = size; i > index; i--) {
                e = e.previous;
            }
        }
        return e;
    }

    public E remove() {
        return removeFirst();
    }

    /**
     * Returns a list-iterator of the elements in this list (in proper
     * sequence), starting at the specified position in the list.
     * Obeys the general contract of <tt>List.listIterator(int)</tt>.<p>
     * <p/>
     * The list-iterator is <i>fail-fast</i>: if the list is structurally
     * modified at any time after the Iterator is created, in any way except
     * through the list-iterator's own <tt>remove</tt> or <tt>add</tt>
     * methods, the list-iterator will throw a
     * <tt>ConcurrentModificationException</tt>.  Thus, in the face of
     * concurrent modification, the iterator fails quickly and cleanly, rather
     * than risking arbitrary, non-deterministic behavior at an undetermined
     * time in the future.
     *
     * @param index index of the first element to be returned from the
     *              list-iterator (by a call to <tt>next</tt>)
     * @return a ListIterator of the elements in this list (in proper
     *         sequence), starting at the specified position in the list
     * @throws IndexOutOfBoundsException {@inheritDoc}
     * @see java.util.List#listIterator(int)
     */
    public ListIterator<E> listIterator(int index) {
        return new ListItr(index);
    }

    private class ListItr implements ListIterator<E> {
        private Entry<E> lastReturned = header;
        private Entry<E> next;
        private int nextIndex;

        ListItr(int index) {
            if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException("Index: " + index +
                        ", Size: " + size);
            }
            if (index < size >> 1) {
                next = header.next;
                for (nextIndex = 0; nextIndex < index; nextIndex++) {
                    next = next.next;
                }
            } else {
                next = header;
                for (nextIndex = size; nextIndex > index; nextIndex--) {
                    next = next.previous;
                }
            }
        }

        public boolean hasNext() {
            return nextIndex != size;
        }

        public E next() {
            if (nextIndex == size) {
                throw new NoSuchElementException();
            }
            lastReturned = next;
            next = next.next;
            nextIndex++;
            return lastReturned.element;
        }

        public boolean hasPrevious() {
            return nextIndex != 0;
        }

        public E previous() {
            if (nextIndex == 0) {
                throw new NoSuchElementException();
            }

            lastReturned = next = next.previous;
            nextIndex--;
            return lastReturned.element;
        }

        public int nextIndex() {
            return nextIndex;
        }

        public int previousIndex() {
            return nextIndex - 1;
        }

        public void remove() {
            Entry<E> lastNext = lastReturned.next;
            try {
                LinkedList.this.remove(lastReturned);
            } catch (NoSuchElementException e) {
                throw new IllegalStateException();
            }
            if (next == lastReturned) {
                next = lastNext;
            } else {
                nextIndex--;
            }
            lastReturned = header;
        }

        public void set(E e) {
            if (lastReturned == header) {
                throw new IllegalStateException();
            }
            lastReturned.element = e;
        }

        public void add(E e) {
            lastReturned = header;
            addBefore(next, e);
            nextIndex++;
        }
    }

    public static class Entry<E> {
        
        E element;
        Entry<E> next;
        Entry<E> previous;

        protected Entry(E element) {
            this.element = element;
        }

        protected void addBefore(Entry<E> entry) {
            next = entry;
            previous = entry.previous;
            previous.next = this;
            entry.previous = this;
        }
    }

    public synchronized Entry<E> addBefore(Entry<E> entry, E e) {
        Entry<E> newEntry = newEntry(e);
        newEntry.addBefore(entry);
        size++;
        return newEntry;
    }

    protected Entry<E> newEntry(E e) {
        return new Entry<E>(e);
    }

    private E remove(Entry<E> e) {
        if (e == header) {
            throw new NoSuchElementException();
        }
        E result = e.element;
        e.previous.next = e.next;
        e.next.previous = e.previous;
        e.next = e.previous = null;
        e.element = null;
        size--;
        return result;
    }

    public Object[] toArray() {
        Object[] result = new Object[size];
        int i = 0;
        for (Entry<E> e = header.next; e != header; e = e.next) {
            result[i++] = e.element;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        if (a.length < size) {
            a = (T[]) java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), size);
        }
        int i = 0;
        Object[] result = a;
        for (Entry<E> e = header.next; e != header; e = e.next) {
            result[i++] = e.element;
        }

        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

    public Iterator<E> iterator() {
        return listIterator();
    }

    public ListIterator<E> listIterator() {
        return listIterator(0);
    }
}
